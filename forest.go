package utreexo

import (
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/exp/slices"
)

// Assert that Forest implements the Utreexo interface.
var _ Utreexo = (*Forest)(nil)

// positionMap value packing: upper 17 bits = addIndex, lower 47 bits = position
// Supports up to 2^47 (~140 trillion) leaves and 2^17 (131,072) adds per block.
const (
	positionBits = 47
	positionMask = (1 << positionBits) - 1
)

func packPosIndex(pos uint64, addIndex int32) uint64 {
	return (uint64(uint32(addIndex)) << positionBits) | (pos & positionMask)
}

func unpackPos(v uint64) uint64  { return v & positionMask }
func unpackIndex(v uint64) int32 { return int32(v >> positionBits) }

// deletedFileHeaderSize is the size of the header in deletedFile.
// Byte 0: recordMode flag, Bytes 1-7: reserved.
const deletedFileHeaderSize = 8

// Forest is a Utreexo accumulator backed by a contiguous file.
// All nodes are stored at position-based offsets (position * 32 bytes).
// Deleted leaves remain in the file for Undo support.
//
// Unlike Pollard which uses pointer-based trees, Forest stores everything
// in a flat file where position N maps to file offset N*32 bytes.
// Deletions work by copying the sibling hash to the parent position.
//
// Forest uses a fixed forestRows to ensure stable position mappings.
// This is set at creation time based on expected maximum leaves.
type Forest struct {
	file         io.ReadWriteSeeker
	deletedFile  io.ReadWriteSeeker // separate file tracking deleted leaf positions + recordMode header
	addIndexFile io.ReadWriteSeeker // stores int32 addIndex at offset pos*4
	NumLeaves    uint64
	forestRows   uint8 // Fixed maximum rows for stable position mapping

	// positionMap maps leaf hashes to packed (addIndex, position) values.
	// Upper 17 bits = addIndex (index within Modify batch), lower 47 bits = position.
	// Only tracks leaves (row 0), not intermediate nodes.
	positionMap map[miniHash]uint64

	// deletedLeafPositions tracks which leaf positions have been deleted.
	// This is persisted to deletedFile so that on restart we don't re-add
	// deleted leaves to the positionMap.
	deletedLeafPositions map[uint64]struct{}

	// recordMode is true after Record is called but before HashAll completes.
	// Calling Modify while in record mode would corrupt the tree.
	// Persisted to deletedFile header (byte 0).
	recordMode bool
}

// NewForest creates a new Forest backed by the given file.
// The file should be an io.ReadWriteSeeker (e.g., *os.File).
// deletedFile tracks deleted leaf positions and stores the recordMode flag in its header.
// addIndexFile stores the addIndex for each leaf.
// numLeaves indicates how many leaves have already been added (0 for new forest).
// forestRows sets the maximum tree height (determines max leaves = 2^forestRows).
//
// If numLeaves > 0, the positionMap is rebuilt by reading all leaf hashes from
// the file, skipping positions recorded in deletedFile.
func NewForest(file, deletedFile, addIndexFile io.ReadWriteSeeker, numLeaves uint64, forestRows uint8) (*Forest, error) {
	if deletedFile == nil || file == nil || addIndexFile == nil {
		return nil, fmt.Errorf("one of the given files are nil")
	}

	f := &Forest{
		file:                 file,
		deletedFile:          deletedFile,
		addIndexFile:         addIndexFile,
		NumLeaves:            numLeaves,
		forestRows:           forestRows,
		positionMap:          make(map[miniHash]uint64, 2<<forestRows),
		deletedLeafPositions: make(map[uint64]struct{}),
	}

	// Rebuild deletedLeafPositions and recordMode from deletedFile
	if err := f.loadDeletedPositions(); err != nil {
		return nil, fmt.Errorf("load deleted positions: %w", err)
	}

	// Rebuild positionMap from existing leaves, skipping deleted positions
	for pos := uint64(0); pos < numLeaves; pos++ {
		if _, deleted := f.deletedLeafPositions[pos]; deleted {
			continue
		}
		hash, err := f.readHash(pos)
		if err != nil {
			return nil, fmt.Errorf("rebuild positionMap at %d: %w", pos, err)
		}
		// Only add non-empty hashes to the map
		if hash != empty {
			addIndex, err := f.readAddIndex(pos)
			if err != nil {
				return nil, fmt.Errorf("rebuild addIndex at %d: %w", pos, err)
			}
			f.positionMap[hash.mini()] = packPosIndex(pos, addIndex)
		}
	}

	return f, nil
}

// loadDeletedPositions reads the header and all deleted leaf positions from deletedFile.
// File format: [8-byte header: byte 0 = recordMode, bytes 1-7 reserved][uint64 positions...]
func (f *Forest) loadDeletedPositions() error {
	// Get file size first
	size, err := f.deletedFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if size == 0 {
		// Empty file, initialize header
		return f.saveRecordMode()
	}

	// Seek back to start and read header
	_, err = f.deletedFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// Read recordMode from byte 0
	var mode byte
	err = binary.Read(f.deletedFile, binary.LittleEndian, &mode)
	if err != nil {
		return err
	}
	f.recordMode = mode != 0

	// Skip rest of header
	_, err = f.deletedFile.Seek(deletedFileHeaderSize, io.SeekStart)
	if err != nil {
		return err
	}

	// Read all deleted position entries
	numEntries := (size - deletedFileHeaderSize) / 8
	for i := int64(0); i < numEntries; i++ {
		var pos uint64
		err := binary.Read(f.deletedFile, binary.LittleEndian, &pos)
		if err != nil {
			return err
		}
		f.deletedLeafPositions[pos] = struct{}{}
	}

	return nil
}

// saveRecordMode writes the recordMode to deletedFile header (byte 0).
func (f *Forest) saveRecordMode() error {
	_, err := f.deletedFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// Write 8-byte header (recordMode in byte 0, rest reserved)
	var header [deletedFileHeaderSize]byte
	if f.recordMode {
		header[0] = 1
	}
	_, err = f.deletedFile.Write(header[:])
	return err
}

// recordDeletedPosition appends a deleted leaf position to the deletedFile.
func (f *Forest) recordDeletedPosition(pos uint64) error {
	// Seek to end of file
	_, err := f.deletedFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	return binary.Write(f.deletedFile, binary.LittleEndian, pos)
}

// popDeletedPositions reads and removes the last n positions from deletedFile.
// Returns positions in the order they were deleted (oldest first for the popped batch).
func (f *Forest) popDeletedPositions(n int) ([]uint64, error) {
	if n == 0 {
		return nil, nil
	}

	// Get current file size
	endOffset, err := f.deletedFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	// Account for header when calculating entries
	totalEntries := (endOffset - deletedFileHeaderSize) / 8
	if int64(n) > totalEntries {
		return nil, fmt.Errorf("popDeletedPositions: requested %d but only %d recorded", n, totalEntries)
	}

	// Seek to start of the last n entries
	startOffset := endOffset - int64(n)*8
	_, err = f.deletedFile.Seek(startOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Read the positions
	positions := make([]uint64, n)
	for i := 0; i < n; i++ {
		err := binary.Read(f.deletedFile, binary.LittleEndian, &positions[i])
		if err != nil {
			return nil, err
		}
	}

	// Truncate the file to remove these entries
	truncater, ok := f.deletedFile.(interface{ Truncate(int64) error })
	if !ok {
		return nil, fmt.Errorf("deletedFile does not support Truncate")
	}
	err = truncater.Truncate(startOffset)
	if err != nil {
		return nil, err
	}

	return positions, nil
}

// GetNumLeaves returns the total number of leaves added to the forest.
func (f *Forest) GetNumLeaves() uint64 {
	return f.NumLeaves
}

// GetTreeRows returns the fixed number of rows in the forest.
func (f *Forest) GetTreeRows() uint8 {
	return TreeRows(f.NumLeaves)
}

// readHash reads the hash at the given position from the file.
func (f *Forest) readHash(position uint64) (Hash, error) {
	offset := int64(position * 32)
	_, err := f.file.Seek(offset, io.SeekStart)
	if err != nil {
		return Hash{}, fmt.Errorf("seek to position %d: %w", position, err)
	}

	var hash Hash
	n, err := f.file.Read(hash[:])
	if err != nil {
		return Hash{}, fmt.Errorf("read at position %d: %w", position, err)
	}
	if n != 32 {
		return Hash{}, fmt.Errorf("short read at position %d: got %d bytes", position, n)
	}

	return hash, nil
}

// writeHash writes the hash at the given position to the file.
func (f *Forest) writeHash(position uint64, hash Hash) error {
	offset := int64(position * 32)
	_, err := f.file.Seek(offset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek to position %d: %w", position, err)
	}

	n, err := f.file.Write(hash[:])
	if err != nil {
		return fmt.Errorf("write at position %d: %w", position, err)
	}
	if n != 32 {
		return fmt.Errorf("short write at position %d: wrote %d bytes", position, n)
	}

	return nil
}

// readAddIndex reads the addIndex for the leaf at the given position.
func (f *Forest) readAddIndex(pos uint64) (int32, error) {
	offset := int64(pos * 4)
	_, err := f.addIndexFile.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	var addIndex int32
	err = binary.Read(f.addIndexFile, binary.LittleEndian, &addIndex)
	if err != nil {
		return 0, err
	}

	return addIndex, nil
}

// writeAddIndex writes the addIndex for the leaf at the given position.
func (f *Forest) writeAddIndex(pos uint64, addIndex int32) error {
	offset := int64(pos * 4)
	_, err := f.addIndexFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	return binary.Write(f.addIndexFile, binary.LittleEndian, addIndex)
}

// add adds a single leaf to the forest.
// If hash is empty, the sibling (existing root) moves up to the parent position.
func (f *Forest) add(hash Hash, addIndex int32) error {
	// Add to position map (before incrementing NumLeaves)
	if hash != empty {
		f.positionMap[hash.mini()] = packPosIndex(f.NumLeaves, addIndex)

		// Write the leaf hash at position NumLeaves
		err := f.writeHash(f.NumLeaves, hash)
		if err != nil {
			return fmt.Errorf("write leaf: %w", err)
		}

		// Write the addIndex
		err = f.writeAddIndex(f.NumLeaves, addIndex)
		if err != nil {
			return fmt.Errorf("write addIndex: %w", err)
		}
	}

	currentHash := hash
	currentPos := f.NumLeaves

	// Hash with existing roots based on binary representation of NumLeaves.
	// Wherever there's a 1 bit, there's an existing root to hash with.
	for h := uint8(0); (f.NumLeaves>>h)&1 == 1; h++ {
		// Get the existing root position at row h
		rootPos := rootPosition(f.NumLeaves, h, f.forestRows)

		// Read the existing root hash
		rootHash, err := f.readHash(rootPos)
		if err != nil {
			return fmt.Errorf("read root at row %d: %w", h, err)
		}

		// Check if this root is a deleted leaf - treat as empty if so
		if _, deleted := f.deletedLeafPositions[rootPos]; deleted {
			rootHash = empty
		}

		// Calculate parent position
		parentPos := Parent(currentPos, f.forestRows)

		// Handle empty hashes using parentHash logic
		newHash := parentHash(rootHash, currentHash)

		// Write the parent hash
		err = f.writeHash(parentPos, newHash)
		if err != nil {
			return fmt.Errorf("write parent at position %d: %w", parentPos, err)
		}

		currentHash = newHash
		currentPos = parentPos
	}

	f.NumLeaves++
	return nil
}

func (f *Forest) delete(delHashes []Hash) error {
	if len(delHashes) == 0 {
		return nil
	}

	for _, delHash := range delHashes {
		_, found := f.positionMap[delHash.mini()]
		if !found {
			return fmt.Errorf("delhash %v not found in position map", delHash)
		}
	}

	for _, delHash := range delHashes {
		err := f.deleteSingle(delHash)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *Forest) deleteSingle(delHash Hash) error {
	// Get the original leaf position before any move-ups
	leafPos := unpackPos(f.positionMap[delHash.mini()])

	// Record deletion: persist to file and track in memory
	if err := f.recordDeletedPosition(leafPos); err != nil {
		return fmt.Errorf("record deleted position %d: %w", leafPos, err)
	}
	f.deletedLeafPositions[leafPos] = struct{}{}

	// First I need to check if I moved up.
	pos := leafPos
	parentPos := Parent(pos, f.forestRows)
	pHash, err := f.readHash(parentPos)
	if err != nil {
		return err
	}

	for pHash == delHash {
		pos = parentPos
		parentPos = Parent(pos, f.forestRows)
		pHash, err = f.readHash(parentPos)
		if err != nil {
			return err
		}
	}

	if isRootPositionTotalRows(pos, f.NumLeaves, f.forestRows) {
		row := DetectRow(pos, f.forestRows)
		if row == 0 {
			// Don't overwrite - just mark as deleted (already done above).
			// The hash is preserved for undo. GetRoots will return empty for deleted roots.
		} else {
			err = f.writeHash(pos, empty)
			if err != nil {
				return err
			}
		}
		return nil
	}

	sibPos := sibling(pos)
	sibHash, err := f.readHash(sibPos)
	if err != nil {
		return err
	}

	// Then I write my hash.
	err = f.writeHash(parentPos, sibHash)
	if err != nil {
		return err
	}
	sibHashPos := parentPos // track where sibHash is written

	// After that, I need to check if my parents moved up.
	parentPos = Parent(parentPos, f.forestRows)
	curHash := pHash
	pHash, err = f.readHash(parentPos)
	if err != nil {
		return err
	}

	for curHash == pHash {
		err = f.writeHash(parentPos, sibHash)
		if err != nil {
			return err
		}
		sibHashPos = parentPos // update position where sibHash lives
		parentPos = Parent(parentPos, f.forestRows)
		pHash, err = f.readHash(parentPos)
		if err != nil {
			return err
		}
	}

	err = f.rehashToRoot(sibHashPos, sibHash)
	if err != nil {
		return err
	}

	return nil
}

// rehashToRoot rehashes from the given position up to the root.
// The hash at pos is provided to avoid an extra read.
func (f *Forest) rehashToRoot(pos uint64, hash Hash) error {
	currentHash := hash
	// Track the original hash at our position (before any writes).
	// This is used to detect if sibling was deleted: if parent == originalPosHash,
	// it means our old hash moved up when sibling was deleted.
	originalPosHash := hash

	for !isRootPositionTotalRows(pos, f.NumLeaves, f.forestRows) {
		parentPos := Parent(pos, f.forestRows)
		parentH, err := f.readHash(parentPos)
		if err != nil {
			return fmt.Errorf("read parent at %d: %w", parentPos, err)
		}

		// Check if sibling was deleted: if parent contains our original hash,
		// it means our hash moved up when sibling was deleted.
		// We should just propagate currentHash up (continue the move-up).
		if parentH == originalPosHash {
			err = f.writeHash(parentPos, currentHash)
			if err != nil {
				return fmt.Errorf("write parent at %d: %w", parentPos, err)
			}
			// For next iteration: the original hash at parentPos was parentH (= originalPosHash)
			// Since we moved up, the chain continues
			originalPosHash = parentH
			pos = parentPos
			continue
		}

		sibPos := sibling(pos)
		sibHash, err := f.readHash(sibPos)
		if err != nil {
			return fmt.Errorf("read sibling at %d: %w", sibPos, err)
		}

		var newParentHash Hash
		if isLeftNiece(pos) {
			newParentHash = parentHash(currentHash, sibHash)
		} else {
			newParentHash = parentHash(sibHash, currentHash)
		}

		// Save the old parent hash before overwriting - we need it for next iteration's check
		oldParentH := parentH

		err = f.writeHash(parentPos, newParentHash)
		if err != nil {
			return fmt.Errorf("write parent at %d: %w", parentPos, err)
		}

		pos = parentPos
		currentHash = newParentHash
		originalPosHash = oldParentH // This was at pos before we wrote
	}

	return nil
}

// GetRoots returns the current root hashes of the forest.
// Implements the ToString interface.
func (f *Forest) GetRoots() []Hash {
	rootPositions := RootPositions(f.NumLeaves, f.forestRows)

	roots := make([]Hash, len(rootPositions))
	for i, pos := range rootPositions {
		// Check if this root position is a deleted leaf - return empty if so
		if _, deleted := f.deletedLeafPositions[pos]; deleted {
			roots[i] = empty
			continue
		}
		hash, err := f.readHash(pos)
		if err != nil {
			roots[i] = empty
		} else {
			roots[i] = hash
		}
	}

	return roots
}

func (f *Forest) getHash(pos uint64) Hash {
	// Tree is the root the position is located under.
	// branchLen denotes how far down the root the position is.
	// bits tell us if we should go down to the left child or the right child.
	if pos >= maxPosition(TreeRows(f.NumLeaves)) {
		return empty
	}
	tree, branchLen, _, err := DetectOffset(pos, f.NumLeaves)
	if err != nil {
		return empty
	}
	if tree >= numRoots(f.NumLeaves) {
		return empty
	}

	startPos := RootPositions(f.NumLeaves, f.forestRows)[tree]
	startHash, err := f.readHash(startPos)
	if err != nil {
		return empty
	}

	// If the root is empty, then everything else is empty.
	if startHash == empty {
		return empty
	}

	// If the root is at row 0, check if it's been deleted as the hash will not be empty.
	if DetectRow(startPos, f.forestRows) == 0 {
		if _, deleted := f.deletedLeafPositions[startPos]; deleted {
			return empty
		}

		// If the hash isn't empty we can just return here.
		return startHash
	}

	for h := int(branchLen) - 1; h >= 0; h-- {
		// If we're at this stage, that means we need to go down. But if the position is at row 0,
		// we can't go down any further.  This indicates that the position is already deleted.
		if DetectRow(startPos, f.forestRows) == 0 {
			return empty
		}

		lChildHash, err := f.readHash(LeftChild(startPos, f.forestRows))
		if err != nil {
			return empty
		}
		rChildHash, err := f.readHash(RightChild(startPos, f.forestRows))
		if err != nil {
			return empty
		}

		// Skip a branch if the hashes are the same. This means that the current root doesn't actually exist.
		switch startHash {
		case lChildHash:
			startPos = LeftChild(startPos, f.forestRows)
			startHash = lChildHash
			h++

			continue

		case rChildHash:
			startPos = RightChild(startPos, f.forestRows)
			startHash = rChildHash
			h++

			continue
		}

		// Figure out which node we need to follow.
		childPos := uint8(pos>>h) & 1
		if isLeftNiece(uint64(childPos)) {
			startPos = LeftChild(startPos, f.forestRows)
			startHash = lChildHash
		} else {
			startPos = RightChild(startPos, f.forestRows)
			startHash = rChildHash
		}
	}

	return startHash
}

// GetHash returns the hash at the given position (in forestRows space).
// Implements the ToString interface for debugging.
// Traverses from root to target position, following the tree structure
// and handling move-ups where a child's hash equals its parent's hash.
func (f *Forest) GetHash(position uint64) Hash {
	return f.getHash(position)
}

// Undo reverts a modification done by Modify.
// This implements the Utreexo interface.
func (f *Forest) Undo(prevAdds []Hash, proof Proof, delHashes, prevRoots []Hash) error {
	if f.recordMode {
		return fmt.Errorf("cannot call Undo while in record mode; call HashAll first")
	}
	return f.undoInternal(uint64(len(prevAdds)), len(delHashes))
}

// undoInternal reverts additions and deletions.
// numAdds is how many leaves were added in the block being undone.
// numDels is how many leaves were deleted in the block being undone.
// Deletion positions and hashes are read from internal files.
func (f *Forest) undoInternal(numAdds uint64, numDels int) error {
	// Step 1: Undo additions
	f.NumLeaves -= numAdds

	// Step 2: Read deleted positions from deletedFile (last N entries)
	delPositions, err := f.popDeletedPositions(numDels)
	if err != nil {
		return fmt.Errorf("pop deleted positions: %w", err)
	}
	if len(delPositions) != numDels {
		return fmt.Errorf("expected %d deleted positions, got %d", numDels, len(delPositions))
	}

	// Step 3: Undo deletions in reverse order (LIFO)
	for i := len(delPositions) - 1; i >= 0; i-- {
		pos := delPositions[i]
		delete(f.deletedLeafPositions, pos)

		err = f.undoSingleDeletion(pos)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *Forest) undoSingleDeletion(pos uint64) error {
	if isRootPositionTotalRows(pos, f.NumLeaves, f.forestRows) {
		return nil
	}

	// Undoing a single deletion is in 5 steps,
	//
	// 1: We grab the current position of the deleted hash as it may have moved up.
	// 2: We calculate the parent hash with the sibling (calculated from the current position).
	// 3: We insert the parent hash into the parent position.
	// 4: We check if the old hash in the parent position moved up as well, inserting theh parent hash
	//    whereever needed.
	// 5: From the last position we've inserted the parent hash (calculated in 3), we rehash to the root
	//    (while keeping in mind to move up hashes if the sibling is deleted).
	hash, err := f.readHash(pos)
	if err != nil {
		return err
	}

	parentPos := Parent(pos, f.forestRows)
	pHash, err := f.readHash(parentPos)
	if err != nil {
		return err
	}

	for pHash == hash {
		pos = parentPos
		parentPos = Parent(pos, f.forestRows)
		pHash, err = f.readHash(parentPos)
		if err != nil {
			return err
		}
	}

	// Check if we already wrote to the root.
	if isRootPositionTotalRows(parentPos, f.NumLeaves, f.forestRows) {
		pHash, err := f.readHash(parentPos)
		if err != nil {
			return err
		}

		// If the parent is a root and it's empty, that means my sib got deleted too.
		if pHash == empty {
			err = f.writeHash(parentPos, hash)
			return err
		}
	}

	sibPos := sibling(pos)
	sibHash, err := f.readHash(sibPos)
	if err != nil {
		return err
	}

	// Calculate parent hash (order depends on left/right)
	var newParentHash Hash
	if isLeftNiece(pos) {
		newParentHash = parentHash(hash, sibHash)
	} else {
		newParentHash = parentHash(sibHash, hash)
	}

	lastWritePos := parentPos
	prevHash := pHash

	err = f.writeHash(lastWritePos, newParentHash)
	if err != nil {
		return err
	}

	// Check if we already wrote to the root
	if isRootPositionTotalRows(lastWritePos, f.NumLeaves, f.forestRows) {
		return nil
	}

	parentPos = Parent(lastWritePos, f.forestRows)
	pHash, err = f.readHash(parentPos)
	if err != nil {
		return err
	}

	// Step 4: Check if the old hash in the parent position moved up as well
	for prevHash == pHash {
		err = f.writeHash(parentPos, newParentHash)
		if err != nil {
			return err
		}
		lastWritePos = parentPos

		if isRootPositionTotalRows(parentPos, f.NumLeaves, f.forestRows) {
			return nil
		}

		parentPos = Parent(parentPos, f.forestRows)
		pHash, err = f.readHash(parentPos)
		if err != nil {
			return err
		}
	}

	// Step 5: Rehash from the last write position to the root
	return f.rehashToRootFromPos(lastWritePos)
}

// rehashToRootFromPos recalculates hashes from the given position up to its root.
func (f *Forest) rehashToRootFromPos(pos uint64) error {
	currentPos := pos

	for !isRootPositionTotalRows(currentPos, f.NumLeaves, f.forestRows) {
		// Read current hash
		currentHash, err := f.readHash(currentPos)
		if err != nil {
			return fmt.Errorf("read current at %d: %w", currentPos, err)
		}

		parentPos := Parent(currentPos, f.forestRows)
		parentH, err := f.readHash(parentPos)
		if err != nil {
			return fmt.Errorf("read parent at %d: %w", parentPos, err)
		}

		// If parent equals current hash, this node has "moved up" due to sibling deletion.
		// Skip this level - the hash is already in the right place.
		if parentH == currentHash {
			currentPos = parentPos
			continue
		}

		// Read sibling hash
		sibPos := sibling(currentPos)
		sibHash, err := f.readHash(sibPos)
		if err != nil {
			return fmt.Errorf("read sibling at %d: %w", sibPos, err)
		}

		// Calculate parent hash (order depends on left/right)
		var newParentHash Hash
		if isLeftNiece(currentPos) {
			newParentHash = parentHash(currentHash, sibHash)
		} else {
			newParentHash = parentHash(sibHash, currentHash)
		}

		// Write to parent
		err = f.writeHash(parentPos, newParentHash)
		if err != nil {
			return fmt.Errorf("write parent at %d: %w", parentPos, err)
		}

		currentPos = parentPos

		prevHash := parentH
		parentPos = Parent(parentPos, f.forestRows)
		parentH, err = f.readHash(parentPos)
		if err != nil {
			return fmt.Errorf("read parent at %d: %w", parentPos, err)
		}
		for prevHash == parentH {
			// Write to parent
			err = f.writeHash(parentPos, newParentHash)
			if err != nil {
				return fmt.Errorf("write parent at %d: %w", parentPos, err)
			}

			currentPos = parentPos

			parentPos = Parent(parentPos, f.forestRows)
			parentH, err = f.readHash(parentPos)
			if err != nil {
				return fmt.Errorf("read parent at %d: %w", parentPos, err)
			}
		}
	}

	return nil
}

// Modify adds and deletes elements from the forest.
// This implements the Utreexo interface.
func (f *Forest) Modify(adds []Leaf, delHashes []Hash, _ Proof) error {
	if f.recordMode {
		return fmt.Errorf("cannot call Modify while in record mode; call HashAll first")
	}

	// Delete first.
	if len(delHashes) > 0 {
		err := f.delete(delHashes)
		if err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}

	// Then add.
	for i, leaf := range adds {
		err := f.add(leaf.Hash, int32(i))
		if err != nil {
			return fmt.Errorf("add: %w", err)
		}
	}

	return nil
}

// ModifyAndReturnTTLs adds and deletes elements from the forest, returning
// the addIndex for each deleted leaf. This allows computing TTL (time-to-live)
// for deleted leaves. Returns -1 for leaves added via Ingest or without TTL tracking.
func (f *Forest) ModifyAndReturnTTLs(adds []Leaf, delHashes []Hash, _ Proof) ([]int32, error) {
	if f.recordMode {
		return nil, fmt.Errorf("cannot call ModifyAndReturnTTLs while in record mode; call HashAll first")
	}

	// Collect addIndexes before deletion
	addIndexes := make([]int32, 0, len(delHashes))
	for _, delHash := range delHashes {
		packed, found := f.positionMap[delHash.mini()]
		if !found {
			return nil, fmt.Errorf("missing %v in positionMap. Cannot return ttls", delHash)
		}
		addIndexes = append(addIndexes, unpackIndex(packed))
	}

	// Delete first.
	if len(delHashes) > 0 {
		err := f.delete(delHashes)
		if err != nil {
			return nil, fmt.Errorf("delete: %w", err)
		}
	}

	// Then add.
	for i, leaf := range adds {
		err := f.add(leaf.Hash, int32(i))
		if err != nil {
			return nil, fmt.Errorf("add: %w", err)
		}
	}

	return addIndexes, nil
}

// Record adds and deletes elements without computing parent hashes.
// Use during IBD for performance - call HashAll() when done to build the tree.
// This is equivalent to Modify but defers all hashing until HashAll().
// Returns the addIndexes for deleted leaves (for TTL tracking).
func (f *Forest) Record(adds []Hash, delHashes []Hash) ([]int32, error) {
	// Collect addIndexes and track deletions
	addIndexes := make([]int32, 0, len(delHashes))
	for _, delHash := range delHashes {
		packed, found := f.positionMap[delHash.mini()]
		if !found {
			return nil, fmt.Errorf("delhash %v not found in position map", delHash)
		}
		addIndexes = append(addIndexes, unpackIndex(packed))
		leafPos := unpackPos(packed)

		if err := f.recordDeletedPosition(leafPos); err != nil {
			return nil, fmt.Errorf("record deleted position %d: %w", leafPos, err)
		}
		f.deletedLeafPositions[leafPos] = struct{}{}
	}

	// Store leaves without computing parent hashes
	for i, hash := range adds {
		if hash != empty {
			f.positionMap[hash.mini()] = packPosIndex(f.NumLeaves, int32(i))
		}

		// Always write the leaf hash (even if empty, so HashAll can read it)
		err := f.writeHash(f.NumLeaves, hash)
		if err != nil {
			return nil, fmt.Errorf("write leaf: %w", err)
		}

		// Write the addIndex
		err = f.writeAddIndex(f.NumLeaves, int32(i))
		if err != nil {
			return nil, fmt.Errorf("write addIndex: %w", err)
		}

		f.NumLeaves++
	}

	f.recordMode = true
	if err := f.saveRecordMode(); err != nil {
		return nil, fmt.Errorf("save record mode: %w", err)
	}
	return addIndexes, nil
}

// HashAll computes all parent hashes after Record calls are complete.
// This builds the complete tree by iterating through all leaves and
// treating deleted positions as empty (causing siblings to move up).
func (f *Forest) HashAll() error {
	totalLeaves := f.NumLeaves

	// Reset to rebuild from scratch
	f.NumLeaves = 0

	for pos := uint64(0); pos < totalLeaves; pos++ {
		var hash Hash
		if _, deleted := f.deletedLeafPositions[pos]; deleted {
			hash = empty
		} else {
			var err error
			hash, err = f.readHash(pos)
			if err != nil {
				return fmt.Errorf("read leaf at %d: %w", pos, err)
			}
		}

		// Compute parent hashes (same logic as add but without writing leaf)
		currentHash := hash
		currentPos := f.NumLeaves

		for h := uint8(0); (f.NumLeaves>>h)&1 == 1; h++ {
			rootPos := rootPosition(f.NumLeaves, h, f.forestRows)

			rootHash, err := f.readHash(rootPos)
			if err != nil {
				return fmt.Errorf("read root at row %d: %w", h, err)
			}

			// Check if this root is a deleted leaf - treat as empty if so
			if _, deleted := f.deletedLeafPositions[rootPos]; deleted {
				rootHash = empty
			}

			parentPos := Parent(currentPos, f.forestRows)
			newHash := parentHash(rootHash, currentHash)

			err = f.writeHash(parentPos, newHash)
			if err != nil {
				return fmt.Errorf("write parent at position %d: %w", parentPos, err)
			}

			currentHash = newHash
			currentPos = parentPos
		}

		f.NumLeaves++
	}

	f.recordMode = false
	if err := f.saveRecordMode(); err != nil {
		return fmt.Errorf("save record mode: %w", err)
	}

	return nil
}

// calculatePosition calculates the logical position of the given hash.
func (f *Forest) calculatePosition(hash Hash) (uint64, error) {
	packed, found := f.positionMap[hash.mini()]
	if !found {
		return 0, fmt.Errorf("hash %v not found in the position map", hash)
	}

	currentPos := unpackPos(packed)
	currentHash := hash
	rowsToTop := 0
	leftRightIndicator := uint64(0)
	for !isRootPositionTotalRows(currentPos, f.NumLeaves, f.forestRows) {
		parentPos := Parent(currentPos, f.forestRows)
		parentH, err := f.readHash(parentPos)
		if err != nil {
			return 0, fmt.Errorf("read parent at %d: %w", parentPos, err)
		}

		if parentH == currentHash {
			// Sibling was deleted. We don't mark at all because this level doesn't exist.
		} else {
			if isLeftNiece(currentPos) {
				// Left
				leftRightIndicator <<= 1
			} else {
				// Right
				leftRightIndicator <<= 1
				leftRightIndicator |= 1
			}
			rowsToTop++
		}
		currentPos = parentPos
		currentHash = parentH
	}

	retPos := currentPos
	for i := 0; i < rowsToTop; i++ {
		isRight := uint64(1) << i
		if leftRightIndicator&isRight == isRight {
			retPos = RightChild(retPos, f.forestRows)
		} else {
			retPos = LeftChild(retPos, f.forestRows)
		}
	}

	return retPos, nil
}

// targetPair holds both calculated position (for ordering) and actual position (for reading).
type targetPair struct {
	calcPos   uint64 // Position for sorting and sibling detection (matches pollard)
	actualPos uint64 // Position in forest file for reading hashes
}

// fetchProofHashes fetchses the needed hashes to prove the given delHashes.
func (f *Forest) fetchProofHashes(delHashes []Hash) ([]Hash, error) {
	if len(delHashes) == 0 {
		return nil, nil
	}

	// Build targets with both calculated and actual positions
	targets := make([]targetPair, 0, len(delHashes))
	for _, delHash := range delHashes {
		packed, found := f.positionMap[delHash.mini()]
		if !found {
			return nil, fmt.Errorf("hash %v not found in the position map", delHash)
		}

		calcPos, err := f.calculatePosition(delHash)
		if err != nil {
			return nil, err
		}

		targets = append(targets, targetPair{calcPos: calcPos, actualPos: unpackPos(packed)})
	}

	// Sort by calculated position to match pollard ordering
	slices.SortFunc(targets, func(a, b targetPair) bool {
		return a.calcPos < b.calcPos
	})

	// deTwin using calculated positions, accounting for move-ups in actualPos
	targets, err := f.deTwinPairs(targets)
	if err != nil {
		return nil, err
	}

	proofHashes := make([]Hash, 0, (len(targets) * int(f.forestRows+1)))
	for row := uint8(0); row <= f.forestRows; row++ {
		for i := 0; i < len(targets); i++ {
			calcPos := targets[i].calcPos
			actualPos := targets[i].actualPos

			if calcPos > maxPossiblePosAtRow(row, f.forestRows) {
				continue
			}
			if row != DetectRow(calcPos, f.forestRows) {
				continue
			}
			if isRootPositionOnRowTotalRows(calcPos, f.NumLeaves, row, f.forestRows) {
				continue
			}

			// Walk up actual position to handle move-ups
			actualParentPos := Parent(actualPos, f.forestRows)
			parentH, err := f.readHash(actualParentPos)
			if err != nil {
				return nil, err
			}
			currentHash, err := f.readHash(actualPos)
			if err != nil {
				return nil, err
			}
			for parentH == currentHash {
				actualPos = actualParentPos
				targets[i].actualPos = actualPos
				actualParentPos = Parent(actualPos, f.forestRows)
				parentH, err = f.readHash(actualParentPos)
				if err != nil {
					return nil, err
				}
			}

			// Check if next target is sibling (using calcPos)
			if i+1 < len(targets) && rightSib(calcPos) == targets[i+1].calcPos {
				targets[i].calcPos = Parent(calcPos, f.forestRows)
				targets[i].actualPos = actualParentPos
				i++ // Skip sibling
				continue
			}

			// Read sibling hash from actual sibling position
			proofHash, err := f.readHash(sibling(actualPos))
			if err != nil {
				return nil, err
			}
			proofHashes = append(proofHashes, proofHash)
			targets[i].calcPos = Parent(calcPos, f.forestRows)
			targets[i].actualPos = actualParentPos
		}

		slices.SortFunc(targets, func(a, b targetPair) bool {
			return a.calcPos < b.calcPos
		})
	}

	return proofHashes, nil
}

// deTwinPairs removes sibling pairs from targets using calculated positions.
// deTwinPairs detwins sibling pairs, updating both calcPos and actualPos.
// This is a method on Forest because actualPos needs to account for nodes
// that have moved up due to deletions (parentHash == currentHash).
func (f *Forest) deTwinPairs(targets []targetPair) ([]targetPair, error) {
	for i := 0; i < len(targets)-1; i++ {
		if targets[i].calcPos|1 == targets[i+1].calcPos {
			targets[i].calcPos = Parent(targets[i].calcPos, f.forestRows)

			// Walk up actualPos to find where the node actually is
			actualPos := targets[i].actualPos
			currentHash, err := f.readHash(actualPos)
			if err != nil {
				return nil, err
			}
			parentPos := Parent(actualPos, f.forestRows)
			parentHash, err := f.readHash(parentPos)
			if err != nil {
				return nil, err
			}
			for parentHash == currentHash {
				actualPos = parentPos
				parentPos = Parent(actualPos, f.forestRows)
				parentHash, err = f.readHash(parentPos)
				if err != nil {
					return nil, err
				}
			}
			targets[i].actualPos = parentPos

			targets = append(targets[:i+1], targets[i+2:]...)
			i--
		}
	}

	return targets, nil
}

// Prove generates an inclusion proof for the given hashes.
// This implements the Utreexo interface by looking up positions from hashes.
// Uses the walk-up algorithm to find where each hash currently is (it may have
// moved up due to deletions), then computes the proof positions.
func (f *Forest) Prove(delHashes []Hash) (Proof, error) {
	if len(delHashes) == 0 {
		return Proof{}, nil
	}

	// Find target positions using the walk-up algorithm
	// findTarget returns positions in TreeRows space
	targets := make([]uint64, len(delHashes))
	for i, hash := range delHashes {
		target, err := f.calculatePosition(hash)
		if err != nil {
			return Proof{}, fmt.Errorf("find target for hash %x: %w", hash[:8], err)
		}
		targets[i] = target
	}

	treeRows := TreeRows(f.NumLeaves)
	if treeRows != f.forestRows {
		targets = translatePositions(targets, f.forestRows, treeRows)
	}

	proofHashes, err := f.fetchProofHashes(delHashes)
	if err != nil {
		return Proof{}, err
	}

	// targets are already in TreeRows space
	return Proof{
		Targets: targets,
		Proof:   proofHashes,
	}, nil
}

// Verify verifies that the given hashes and proof are valid.
// This implements the Utreexo interface.
func (f *Forest) Verify(delHashes []Hash, proof Proof, remember bool) error {
	stump := Stump{
		Roots:     f.GetRoots(),
		NumLeaves: f.NumLeaves,
	}
	_, err := Verify(stump, delHashes, proof)
	return err
}

// GetLeafPosition returns the position for the given leaf hash.
// This implements the Utreexo interface.
func (f *Forest) GetLeafPosition(hash Hash) (uint64, bool) {
	packed, ok := f.positionMap[hash.mini()]
	return unpackPos(packed), ok
}

// String returns a string representation of the forest.
// This implements the Utreexo interface.
func (f *Forest) String() string {
	return String(f)
}

// RawString returns a string representation of the raw file contents.
// Unlike String() which accounts for move-ups and returns the logical tree structure,
// RawString() shows the actual bytes stored at each file position. Useful for debugging.
func (f *Forest) RawString() string {
	return String(&rawForest{f})
}

// rawForest wraps Forest to provide raw file reads for GetHash
type rawForest struct {
	*Forest
}

func (r *rawForest) GetTreeRows() uint8 {
	return r.forestRows
}

func (r *rawForest) GetHash(position uint64) Hash {
	hash, err := r.readHash(position)
	if err != nil {
		return empty
	}

	return hash
}
