package codesnap

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/yasyf/synckit/artifact"
)

type fakeStore struct {
	mu      sync.Mutex
	objects map[artifact.Digest][]byte
	writes  int
	tamper  map[artifact.Digest]func([]byte) []byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[artifact.Digest][]byte{}, tamper: map[artifact.Digest]func([]byte) []byte{}}
}

func (f *fakeStore) Put(_ context.Context, r io.Reader, media string) (artifact.Ref, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return artifact.Ref{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	m := artifact.Manifest{Schema: artifact.ManifestSchema, Media: media, Size: int64(len(data))}
	for chunk := range slices.Chunk(data, artifact.ChunkSize) {
		m.Chunks = append(m.Chunks, artifact.ChunkRef{Digest: f.store(chunk), Size: int64(len(chunk))})
	}
	return f.storeManifest(m)
}

func (f *fakeStore) PutGroup(_ context.Context, media string, deps []artifact.Ref) (artifact.Ref, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, dep := range deps {
		if _, ok := f.objects[dep.Digest]; !ok {
			return artifact.Ref{}, &artifact.MissingError{Digest: dep.Digest}
		}
	}
	return f.storeManifest(artifact.Manifest{Schema: artifact.ManifestSchema, Media: media, Deps: deps})
}

func (f *fakeStore) Has(_ context.Context, digests []artifact.Digest) ([]artifact.Digest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var missing []artifact.Digest
	for _, d := range digests {
		if _, ok := f.objects[d]; !ok {
			missing = append(missing, d)
		}
	}
	return missing, nil
}

func (f *fakeStore) Manifest(_ context.Context, ref artifact.Ref) (artifact.Manifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.manifest(ref.Digest)
}

func (f *fakeStore) Open(_ context.Context, ref artifact.Ref) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.manifest(ref.Digest)
	if err != nil {
		return nil, err
	}
	var content []byte
	for _, chunk := range m.Chunks {
		data, ok := f.objects[chunk.Digest]
		if !ok {
			return nil, &artifact.MissingError{Digest: chunk.Digest}
		}
		content = append(content, data...)
	}
	if tamper, ok := f.tamper[ref.Digest]; ok {
		content = tamper(content)
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (f *fakeStore) Complete(_ context.Context, roots []artifact.Ref) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	missing := 0
	seen := map[artifact.Digest]bool{}
	var walk func(d artifact.Digest)
	walk = func(d artifact.Digest) {
		if seen[d] {
			return
		}
		seen[d] = true
		m, err := f.manifest(d)
		if err != nil {
			missing++
			return
		}
		for _, chunk := range m.Chunks {
			if _, ok := f.objects[chunk.Digest]; !ok {
				missing++
			}
		}
		for _, dep := range m.Deps {
			walk(dep.Digest)
		}
	}
	for _, root := range roots {
		walk(root.Digest)
	}
	return missing, nil
}

func (f *fakeStore) closure(root artifact.Ref) map[artifact.Digest]artifact.Manifest {
	f.mu.Lock()
	defer f.mu.Unlock()
	reached := map[artifact.Digest]artifact.Manifest{}
	var walk func(d artifact.Digest)
	walk = func(d artifact.Digest) {
		if _, ok := reached[d]; ok {
			return
		}
		m, err := f.manifest(d)
		if err != nil {
			panic(err)
		}
		reached[d] = m
		for _, dep := range m.Deps {
			walk(dep.Digest)
		}
	}
	walk(root.Digest)
	return reached
}

func (f *fakeStore) remove(d artifact.Digest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, d)
}

func (f *fakeStore) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes
}

func (f *fakeStore) setTamper(d artifact.Digest, tamper func([]byte) []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tamper[d] = tamper
}

func (f *fakeStore) store(data []byte) artifact.Digest {
	d := artifact.Sum(data)
	if _, ok := f.objects[d]; !ok {
		f.objects[d] = bytes.Clone(data)
		f.writes++
	}
	return d
}

func (f *fakeStore) storeManifest(m artifact.Manifest) (artifact.Ref, error) {
	data, err := m.Encode()
	if err != nil {
		return artifact.Ref{}, err
	}
	return artifact.Ref{Digest: f.store(data), Kind: artifact.KindManifest, Size: m.Size}, nil
}

func (f *fakeStore) manifest(d artifact.Digest) (artifact.Manifest, error) {
	data, ok := f.objects[d]
	if !ok {
		return artifact.Manifest{}, &artifact.MissingError{Digest: d}
	}
	m, err := artifact.DecodeManifest(data)
	if err != nil {
		return artifact.Manifest{}, fmt.Errorf("decode manifest %s: %w", d, err)
	}
	return m, nil
}
