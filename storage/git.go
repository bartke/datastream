package storage

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// GitRepository implements the Storage interface for a Git repository
type GitRepository struct {
	repo *git.Repository

	name         string
	email        string
	syncInterval time.Duration
}

type Params struct {
	RepoPath string

	// optional
	Name         string
	Email        string
	SyncInterval time.Duration
}

// NewGitRepository creates a new GitRepository type that implements the Storage interface
func NewGitRepository(p Params) (Storage, error) {
	repo, err := git.PlainOpen(p.RepoPath)
	if err != nil {
		return nil, err
	}

	return &GitRepository{
		repo:         repo,
		name:         p.Name,
		email:        p.Email,
		syncInterval: p.SyncInterval,
	}, nil
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

func (r *GitRepository) Sync(keys []string) (map[string]Data, error) {
	data := make(map[string]Data)

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
	var last time.Time
	go func() {
		defer close(out)
		for {
			ref, err := r.repo.Head()
			if err != nil {
				return
			}

			// timestamp of ref
			c, err := r.repo.CommitObject(ref.Hash())
			if err != nil {
				return
			}

			// if no update has been made since the last sync, skip
			if c.Author.When.Before(last) {
				continue
			}

			tree, err := c.Tree()

			for _, key := range keys {
				file, err := tree.File(key)
				if err != nil {
					continue
				}

				value, err := file.Contents()
				if err != nil {
					return
				}

				out <- Data{
					Key:       key,
					Value:     []byte(value),
					ValueType: "text/plain",
					UpdatedAt: c.Author.When,
				}
			}

			last = time.Now()
			<-time.After(r.syncInterval)
		}
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
	commit, err := w.Commit("Update key", &git.CommitOptions{
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
