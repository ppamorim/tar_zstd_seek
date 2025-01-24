Simply run:

```
cargo run
```

## Challenge

The goal is to modify the `decompress_latest` function to avoid decompressing the binary twice. This might be achievable by leveraging the `Seek` trait from the `tar` crate. However, the current limitation is that `Decode` does not implement `Seek`.

### Objective

The primary objective is to minimize the amount of data processed. Within the `input.tar.zst` archive, files are named sequentially as `0.csv`, `1.csv`, and so on. For this task, only the latest `.csv` file is required, making it inefficient to process the entire archive.