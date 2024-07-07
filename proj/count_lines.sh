#!/bin/bash

# 指定文件扩展名
file_extensions=("*.java")

# 创建一个临时文件来存储过滤后的行
temp_file=$(mktemp)

# 查找并过滤文件
for ext in "${file_extensions[@]}"; do
    find . -type f -name "$ext" -print0 | while IFS= read -r -d '' file; do
        # 过滤掉注释和空行，并附加到临时文件
        grep -vE '^\s*(//|/\*|\*|\*/)' "$file" | grep -vE '^\s*$' >> "$temp_file"
    done
done

# 统计代码行数
line_count=$(wc -l < "$temp_file")

# 删除临时文件
rm "$temp_file"

echo "Total lines of code (excluding comments and empty lines): $line_count"

