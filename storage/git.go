package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
)

// GitRepository implements the Storage interface for a Git repository
type GitRepository struct {
	repo *git.Repository
	auth transport.AuthMethod

	isRemote bool

	name  string
	email string

	syncInterval time.Duration
	errorChannel chan<- error
}

type GitRepositoryConfig struct {
	RepoPath string

	// optional name for commits
	CommitName string
	// optional email for commits
	CommitEmail string
	// optional username for basic auth
	Username string
	// optional basic auth token
	Password string

	// optional sync interval, default is 5 seconds
	SyncInterval time.Duration

	// optional error channel
	ErrorChannel chan<- error
}

// NewGitRepository creates a new GitRepository type that implements the Storage interface
func NewGitRepository(config GitRepositoryConfig) (Storage, error) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	tempDir := filepath.Join(os.TempDir(), timestamp)

	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		return nil, err
	}

	store := &GitRepository{
		name:         config.CommitName,
		email:        config.CommitEmail,
		syncInterval: config.SyncInterval,
		errorChannel: config.ErrorChannel,
	}

	if config.Password != "" {
		store.auth = &http.BasicAuth{
			Username: config.Username,
			Password: config.Password,
		}
	}

	// either local or switch to remote and clone
	repo, err := git.PlainOpen(config.RepoPath)
	if err == git.ErrRepositoryNotExists {
		cfg := &git.CloneOptions{
			URL:          config.RepoPath,
			SingleBranch: true,
			Progress:     ioutil.Discard,
			Auth:         store.auth,
		}
		repo, err = git.PlainClone(tempDir, false, cfg)
		if err != nil {
			return nil, err
		}
		store.isRemote = true
	} else if err != nil {
		return nil, err
	}

	store.repo = repo

	if store.syncInterval == 0 {
		store.syncInterval = DefaultSyncInterval
	}

	return store, nil
}

func (s *GitRepository) forwardError(err error) {
	if s.errorChannel != nil {
		s.errorChannel <- err
	}
}

func (r *GitRepository) ListCapabilities() ([]Capability, error) {
	ref, err := r.repo.Head()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve HEAD reference: %w", err)
	}

	commit, err := r.repo.CommitObject(ref.Hash())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve commit: %w", err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve tree: %w", err)
	}

	var capabilities []Capability
	err = tree.Files().ForEach(func(file *object.File) error {
		// get file extension for value type
		extension := filepath.Ext(file.Name)
		if extension == "" {
			extension = "text"
		}
		capabilities = append(capabilities, Capability{Key: file.Name, ValueType: extension})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to enumerate files: %w", err)
	}

	return capabilities, nil
}

func (r *GitRepository) sync() error {
	if !r.isRemote {
		return nil
	}

	opts := &git.PullOptions{
		Progress: ioutil.Discard,
		Force:    true,
	}

	if r.auth != nil {
		opts.Auth = r.auth
	}

	tree, err := r.repo.Worktree()
	if err != nil {
		return err
	}

	err = tree.Pull(opts)
	if err != nil && err != git.NoErrAlreadyUpToDate {
		return err
	}

	return nil
}

func (r *GitRepository) Sync(keys []string) (map[string]Data, error) {
	data := make(map[string]Data)

	if err := r.sync(); err != nil {
		return nil, fmt.Errorf("failed to sync: %w", err)
	}

	tree, err := r.repo.Worktree()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve tree: %w", err)
	}

	for _, key := range keys {
		// Check if the file exists in the repository
		s, err := tree.Filesystem.Stat(key)
		if err != nil {
			return nil, fmt.Errorf("failed to access file '%s': %v", key, err)
		}

		f, err := tree.Filesystem.Open(key)
		if err != nil {
			return nil, fmt.Errorf("failed to open file '%s': %v", key, err)
		}

		// read file contents
		value := make([]byte, s.Size())
		_, err = f.Read(value)
		if err != nil {
			return nil, fmt.Errorf("failed to read file '%s': %v", key, err)
		}

		// Add the key-value pair to the data map
		data[key] = Data{
			Key:       key,
			Value:     value,
			ValueType: "text/plain",
			UpdatedAt: s.ModTime(),
		}
	}

	return data, nil
}

func (r *GitRepository) Subscribe(keys []string) (<-chan Data, error) {
	out := make(chan Data)

	var repohash string
	filehashes := make(map[string]string)

	go func() {
		defer close(out)
		for {
			if err := r.sync(); err != nil {
				r.forwardError(err)
				// no reason to abort yet - just try again
			}

			ref, err := r.repo.Head()
			if err != nil {
				r.forwardError(fmt.Errorf("failed to retrieve HEAD reference: %w", err))
				break
			}

			c, err := r.repo.CommitObject(ref.Hash())
			if err != nil {
				r.forwardError(fmt.Errorf("failed to retrieve commit: %w", err))
				break
			}

			// if no update has been made since the last sync, skip
			if ref.Hash().String() != repohash {
				tree, err := c.Tree()
				if err != nil {
					r.forwardError(fmt.Errorf("failed to retrieve tree: %w", err))
					return
				}

				for _, key := range keys {
					file, err := tree.File(key)
					if err != nil {
						r.forwardError(fmt.Errorf("failed to retrieve file '%s': %w", key, err))
						continue
					}

					// if no update has been made since the last sync, skip
					if file.Hash.String() == filehashes[key] {
						continue
					}

					value, err := file.Contents()
					if err != nil {
						r.forwardError(fmt.Errorf("failed to retrieve file contents '%s': %w", key, err))
						continue
					}

					out <- Data{
						Key:       key,
						Value:     []byte(value),
						ValueType: "text/plain",
						UpdatedAt: c.Author.When,
					}
					filehashes[key] = file.Hash.String()
				}
			}

			repohash = ref.Hash().String()
			<-time.After(r.syncInterval)
		}
		close(out)
	}()

	return out, nil
}

func (r *GitRepository) PushUpdate(data *Data) error {
	// Get the current branch
	w, err := r.repo.Worktree()
	if err != nil {
		return err
	}

	// Write the data to the file
	err = ioutil.WriteFile(filepath.Join(data.Key), data.Value, 0644)
	if err != nil {
		return err
	}

	// Commit the changes to the branch
	_, err = w.Add(data.Key)
	if err != nil {
		return err
	}

	_, err = w.Commit("Update key", &git.CommitOptions{
		Author: &object.Signature{
			Name:  r.name,
			Email: r.email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return err
	}

	// Push the changes to the remote repository
	err = r.repo.Push(&git.PushOptions{})
	if err != nil {
		return err
	}

	return nil
}
