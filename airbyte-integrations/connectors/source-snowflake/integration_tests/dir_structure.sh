#!/bin/bash

# Base directory
base_dir="configs"

# Define the categories and their possible values
modes=("full_refresh" "incremental")
types=("standard" "history")
streams=("full_stream" "push_down_filter_stream")

# Create the directory structure
for mode in "${modes[@]}"; do
  for type in "${types[@]}"; do
    for stream in "${streams[@]}"; do
      # Construct the directory path
      dir_path="$base_dir/$mode/$type/$stream"

      # Create the directory
      mkdir -p "$dir_path"

      # Create placeholder config.json and catalog.json files
      echo "{}" > "$dir_path/config.json"
      echo "{}" > "$dir_path/catalog.json"
    done
  done
done

echo "Directory structure created successfully."
