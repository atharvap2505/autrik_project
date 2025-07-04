INPUT_DIR="./input_logs"
OUTPUT_DIR="./parsed_logs"
mkdir -p "$OUTPUT_DIR"

for file in "$INPUT_DIR"/*.txt; do
    filename=$(basename "$file" .txt)
    output_file="$OUTPUT_DIR/${filename}_results.json"
    if [ -f "$output_file" ]; then
        echo "Skipping $filename.txt - already processed"
        continue
    fi
    echo "Processing $filename.txt"
    docker run -v "$INPUT_DIR":/tmp pf "/tmp/$filename.txt" > "$output_file"
done