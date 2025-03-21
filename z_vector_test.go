// Copyright 2025 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func compareDenseVector(t *testing.T, id godror.Number, got godror.Vector, expected godror.Vector) {
	t.Helper()
	if !reflect.DeepEqual(got.Values, expected.Values) {
		t.Errorf("ID %v: expected %v, got %v", id, expected.Values, got.Values)
	}
}

func compareSparseVector(t *testing.T, id godror.Number, got godror.Vector, expected godror.Vector) {
	t.Helper()
	if !reflect.DeepEqual(got.Values, expected.Values) {
		t.Errorf("ID %s: Sparse godror.Vector values mismatch. Got %+v, expected %+v", id, got.Values, expected.Values)
	}
	if !reflect.DeepEqual(got.Indices, expected.Indices) {
		t.Errorf("ID %s: Sparse godror.Vector indices mismatch. Got %+v, expected %+v", id, got.Indices, expected.Indices)
	}
	if got.Dimensions != expected.Dimensions {
		t.Errorf("ID %s: Sparse godror.Vector dimensions mismatch. Got %d, expected %d", id, got.Dimensions, expected.Dimensions)
	}
}

// It Verifies returning godror.Vector columns in outbinds
func TestVectorOutBinds(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("OutBindsVector"), 30*time.Second)
	defer cancel()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_vector_outbind" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	_, err = conn.ExecContext(ctx,
		`CREATE TABLE `+tbl+` (
			id NUMBER(6),
			image_vector Vector,
			graph_vector Vector(5, float32, SPARSE),
			int_vector Vector(3, int8),
			float_vector Vector(4, float64),
			binary_vector Vector(16, binary),
			sparse_int_vector Vector(4, int8, SPARSE)
		)`,
	)
	if err != nil {
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Logf("Vector table %q created", tbl)
	defer testDb.Exec("DROP TABLE " + tbl)

	stmt, err := conn.PrepareContext(ctx,
		`INSERT INTO `+tbl+` (id, image_vector, graph_vector, int_vector, float_vector,
		binary_vector, sparse_int_vector)
		 VALUES (:1, :2, :3, :4, :5, :6, :7) RETURNING image_vector, graph_vector,
		 int_vector, float_vector, binary_vector,
      sparse_int_vector INTO :8, :9, :10, :11, :12, :13`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// Test values
	id := godror.Number("1")
	vectors := []godror.Vector{
		{Values: []float32{1.1, 2.2, 3.3}}, // image_vector
		{Values: []float32{0.5, 1.2, -0.9}, Indices: []uint32{0, 2, 3}, Dimensions: 5, IsSparse: true}, // graph_vector
		{Values: []int8{1, -5, 3}},
		{Values: []float64{10.5, 20.3, -5.5, 3.14}},
		{Values: []uint8{255, 100}}, // binary vector
		{Values: []int8{-1, 4, -7}, Indices: []uint32{1, 2, 3}, Dimensions: 4, IsSparse: true}, // sparse_int_vector
	}
	outVectors := make([]godror.Vector, len(vectors))

	_, err = stmt.ExecContext(ctx, 1, vectors[0], vectors[1], vectors[2],
		vectors[3], vectors[4], vectors[5],
		sql.Out{Dest: &outVectors[0]}, sql.Out{Dest: &outVectors[1]},
		sql.Out{Dest: &outVectors[2]}, sql.Out{Dest: &outVectors[3]},
		sql.Out{Dest: &outVectors[4]}, sql.Out{Dest: &outVectors[5]})
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	// Validate out bind values
	for i, v := range vectors {
		if v.IsSparse {
			compareSparseVector(t, id, outVectors[i], v)
		} else {
			compareDenseVector(t, id, outVectors[i], v)
		}
	}
}

// Generates n indices within maxRange
func generateIndexArray(n int, maxRange int) []uint32 {
	var a [32]byte
	crand.Read(a[:])
	rnd := rand.New(rand.Source(rand.NewChaCha8(a)))

	// Generate a permutation of numbers from 0 to maxRange-1
	permutation := rnd.Perm(maxRange)

	// Create a uint32 slice
	indexArray := make([]uint32, n)
	for i := 0; i < n; i++ {
		indexArray[i] = uint32(permutation[i])
	}

	// Sort the array in ascending order as expected by DB
	sort.Slice(indexArray, func(i, j int) bool {
		return indexArray[i] < indexArray[j]
	})

	return indexArray
}

// Helper function to generate random batch data
func generateRandomBatch(size int) ([]godror.Number, []godror.Vector, []godror.Vector, []*godror.Vector, []*godror.Vector) {
	ids := make([]godror.Number, 2*size)
	images := make([]godror.Vector, size)
	graphs := make([]godror.Vector, size)
	imagesPtr := make([]*godror.Vector, size)
	graphsPtr := make([]*godror.Vector, size)
	denseValuesCnt := 10
	sparseValuesCnt := 10
	sparseDimsCnt := 100

	for i := 0; i < size; i++ {
		ids[i] = godror.Number(strconv.Itoa(i))
		images[i] = godror.Vector{Values: randomFloat32Slice(denseValuesCnt)}
		graphs[i] = godror.Vector{
			Values:     randomFloat32Slice(sparseValuesCnt),
			Indices:    generateIndexArray(sparseValuesCnt, sparseDimsCnt),
			Dimensions: uint32(sparseDimsCnt),
			IsSparse:   true,
		}
	}
	for i := 0; i < size; i++ {
		ids[size+i] = godror.Number(strconv.Itoa(size + i))
		imagesPtr[i] = &godror.Vector{Values: randomFloat32Slice(denseValuesCnt + 100)}
		graphsPtr[i] = &godror.Vector{
			Values:     randomFloat32Slice(sparseValuesCnt + 100),
			Indices:    generateIndexArray(sparseValuesCnt+100, sparseDimsCnt+100),
			Dimensions: uint32(sparseDimsCnt + 100),
		}
	}
	return ids, images, graphs, imagesPtr, graphsPtr
}

// Helper function to generate a random single row
func generateRandomRow(id int) (godror.Number, godror.Vector, godror.Vector) {
	return godror.Number(strconv.Itoa(id)),
		godror.Vector{Values: randomFloat32Slice(3)},
		godror.Vector{
			Values:     randomFloat32Slice(3),
			Indices:    []uint32{0, 1, 2},
			Dimensions: 5,
			IsSparse:   true,
		}
}

// Generates a slice of random float32 numbers
func randomFloat32Slice(size int) []float32 {
	slice := make([]float32, size)
	for i := range slice {
		slice[i] = rand.Float32() * 10
	}
	return slice
}

// It Verifies batch insert of Vector columns and verify the inserted rows.
func TestVectorReadWriteBatch(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("ReadWriteVector"), 30*time.Second)
	defer cancel()
	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_vector_batch" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	_, err = conn.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (id NUMBER(6), image_vector Vector, graph_vector Vector(*, float32, SPARSE) )", //nolint:gas
	)
	if err != nil {
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Logf(" Vector table  %q: ", tbl)

	defer testDb.Exec(
		"DROP TABLE " + tbl, //nolint:gas
	)

	stmt, err := conn.PrepareContext(ctx,
		"INSERT INTO "+tbl+" (id, image_vector, graph_vector) VALUES (:1, :2, :3)", //nolint:gas
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// Generate random batch values
	const batchSize = 10
	ids, images, graphs, imagesPtr, graphsPtr := generateRandomBatch(batchSize)

	// Insert batch
	if _, err = stmt.ExecContext(ctx, ids[:batchSize], images, graphs); err != nil {
		t.Fatalf("Batch insert failed: %v", err)
	}

	// Insert batch Pointers
	if _, err = stmt.ExecContext(ctx, ids[batchSize:], imagesPtr, graphsPtr); err != nil {
		t.Fatalf("Batch insert failed: %v", err)
	}

	// Insert a single row
	lastID1, lastImage, lastGraph := generateRandomRow(2 * batchSize)
	if _, err = stmt.ExecContext(ctx, lastID1, lastImage, lastGraph); err != nil {
		t.Fatalf("Single insert failed: %v", err)
	}

	// Insert a single rowPtr
	lastID2, lastImage2, lastGraph2 := generateRandomRow((2 * batchSize) + 1)
	if _, err = stmt.ExecContext(ctx, lastID2, &lastImage2, &lastGraph2); err != nil {
		t.Fatalf("Single insert failed: %v", err)
	}

	// Validate inserted rows
	rows, err := conn.QueryContext(ctx, "SELECT id, image_vector, graph_vector FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatalf("Select query failed: %v", err)
	}
	defer rows.Close()

	expectedIDs := append(ids, lastID1, lastID2)
	expectedImages := images
	expectedGraphs := graphs

	// Include pointer-based vectors
	for i := range imagesPtr {
		expectedImages = append(expectedImages, *imagesPtr[i])
		expectedGraphs = append(expectedGraphs, *graphsPtr[i])
	}

	// Include single rows and pointer single rows
	expectedImages = append(expectedImages, lastImage)
	expectedImages = append(expectedImages, lastImage2)
	expectedGraphs = append(expectedGraphs, lastGraph)
	expectedGraphs = append(expectedGraphs, lastGraph2)

	index := 0
	for rows.Next() {
		var id godror.Number
		var image, graph godror.Vector
		if err := rows.Scan(&id, &image, &graph); err != nil {
			t.Fatalf("Row scan failed: %v", err)
		}

		// Ensure the number of rows matches expectations
		if index >= len(expectedIDs) {
			t.Fatalf("More rows returned than expected! Expected: %d, Got: %d", len(expectedIDs), index+1)
		}

		// Verify vector values (Rows returned may be in same sequence as inserted)
		t.Logf("Verifying row ID: %s", id)
		compareDenseVector(t, expectedIDs[index], image, expectedImages[index])
		compareSparseVector(t, expectedIDs[index], graph, expectedGraphs[index])

		index++
	}
}

// Generates test vectors
func generateTestVectors() (godror.Number, []godror.Vector) {
	return godror.Number("1"), []godror.Vector{
		{Values: []float32{1.1, 2.2, 3.3}}, // image_vector
		{Values: []float32{0.5, 1.2, -0.9}, Indices: []uint32{0, 2, 3}, Dimensions: 5, IsSparse: true}, // graph_vector
		{Values: []int8{1, -5, 3}},                  // int_vector
		{Values: []float64{10.5, 20.3, -5.5, 3.14}}, // float_vector
		{Values: []int8{-1, 4, -7}, Indices: []uint32{1, 2, 3}, Dimensions: 4, IsSparse: true}, // sparse_int_vector
	}
}

// Validates dense and sparse vectors
func validateVectors(t *testing.T, id godror.Number, expected, actual []godror.Vector) {
	compareDenseVector(t, id, actual[0], expected[0])
	compareSparseVector(t, id, actual[1], expected[1])
	compareDenseVector(t, id, actual[2], expected[2])
	compareDenseVector(t, id, actual[3], expected[3])
	compareSparseVector(t, id, actual[4], expected[4])
}

// It Verifies Flex storage godror.Vector columns.
func TestVectorFlex(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("BindsFlexVector"), 30*time.Second)
	defer cancel()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_vector_flexbind" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	_, err = conn.ExecContext(ctx,
		`CREATE TABLE `+tbl+` (
			id NUMBER(6), 
			image_vector Vector(*,*),
			graph_vector Vector(*, *, SPARSE), 
			int_vector Vector(*, *), 
			float_vector Vector(*, *), 
			sparse_int_vector Vector(*, *, SPARSE)
		)`,
	)
	if err != nil {
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Logf("Vector table %q created", tbl)
	defer testDb.Exec("DROP TABLE " + tbl)

	stmt, err := conn.PrepareContext(ctx,
		`INSERT INTO `+tbl+` (id, image_vector, graph_vector, int_vector, float_vector, sparse_int_vector) 
		 VALUES (:1, :2, :3, :4, :5, :6) RETURNING image_vector, graph_vector, int_vector, float_vector, sparse_int_vector INTO :7, :8, :9, :10, :11`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	id, expectedVectors := generateTestVectors()
	var outVectors [5]godror.Vector

	_, err = stmt.ExecContext(ctx, id, expectedVectors[0], expectedVectors[1], expectedVectors[2], expectedVectors[3], expectedVectors[4],
		sql.Out{Dest: &outVectors[0]}, sql.Out{Dest: &outVectors[1]}, sql.Out{Dest: &outVectors[2]}, sql.Out{Dest: &outVectors[3]}, sql.Out{Dest: &outVectors[4]})
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	// Validate inserted values
	validateVectors(t, id, expectedVectors, outVectors[:])

	// Fetch inserted values
	row := conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT image_vector, graph_vector, int_vector, float_vector, sparse_int_vector FROM %s WHERE id = :1`, tbl), id)
	err = row.Scan(&outVectors[0], &outVectors[1], &outVectors[2], &outVectors[3], &outVectors[4])
	if err != nil {
		t.Errorf("Select failed for ID %s: %v", id, err)
	}

	// Validate fetched values
	validateVectors(t, id, expectedVectors, outVectors[:])
}

// It Verifies Passing Pointer to Vector type to avoid copies
func TestVectorPointerCases(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(testContext("VectorErrors"), 30*time.Second)
	defer cancel()

	conn, err := testDb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tbl := "test_vector_pointers" + tblSuffix
	conn.ExecContext(ctx, "DROP TABLE "+tbl)
	_, err = conn.ExecContext(ctx,
		`CREATE TABLE `+tbl+` (
			id NUMBER(6), 
			flex_dense_vector1 Vector(*,*),
			flex_sparse_vector1 Vector(*, *, SPARSE), 
			flex_dense_vector2 Vector(*, *), 
			flex_sparse_vector2 Vector(*, *, SPARSE)
		)`,
	)
	if err != nil {
		if errIs(err, 902, "invalid datatype") {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	t.Logf("Vector table %q created", tbl)
	defer testDb.Exec("DROP TABLE " + tbl)

	stmt, err := conn.PrepareContext(ctx,
		`INSERT INTO `+tbl+` (id, flex_dense_vector1, flex_sparse_vector1, flex_dense_vector2, flex_sparse_vector2) 
		 VALUES (:1, :2, :3, :4, :5) `,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// Test values
	var emptyVector godror.Vector
	var sparseVec1 = godror.Vector{
		Values:     []float32{0.5, 1.2, -0.9},
		Indices:    []uint32{0, 2, 3},
		Dimensions: 5,
	}
	var sparseVec2 = godror.Vector{
		Values:     []int8{1, -5, 3},
		Indices:    []uint32{1, 2, 3},
		Dimensions: 4,
	}
	var nilPtrEmbedding *godror.Vector

	// Execute insertion
	_, err = stmt.ExecContext(ctx, 1, emptyVector, &sparseVec1, nilPtrEmbedding, sparseVec2)
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	// Query results
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, flex_dense_vector1, flex_sparse_vector1, flex_dense_vector2, flex_sparse_vector2 
		FROM %s`, tbl))
	if err != nil {
		t.Errorf("QueryContext failed: %v", err)
		return
	}
	defer rows.Close()

	// Validate results
	var id godror.Number
	var dense1, sparse1, sparse2 godror.Vector
	var dense2 interface{}

	for rows.Next() {
		if err := rows.Scan(&id, &dense1, &sparse1, &dense2, &sparse2); err != nil {
			t.Errorf("Scan failed: %v", err)
			continue
		}

		compareDenseVector(t, id, dense1, emptyVector)
		compareSparseVector(t, id, sparse1, sparseVec1)
		if v, ok := dense2.(godror.Vector); ok {
			compareDenseVector(t, id, v, emptyVector)
		} else {
			t.Errorf("Invalid vector type for dense2: %v", dense2)
		}
		compareSparseVector(t, id, sparse2, sparseVec2)
	}
}
