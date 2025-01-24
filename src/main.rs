use std::{fs, io::{BufReader, Cursor, Read}};

use tar::{Archive, Entry};
use thiserror::Error;
use zstd::Decoder;

#[derive(Debug, Error)]
pub enum DecompressError {
  #[error("Decode error: {0}")]
  DecodeError(#[from] std::io::Error),
  #[error("Missing metadata")]
  MissingMetadata,
}

fn decompress_latest<R: Read>(
  reader: R,
) -> Result<Vec<String>, DecompressError> {

  // Workaround until Decoder doesn't support Seek
  const CHUNK_SIZE: usize = 8192;
  let mut buf_reader = BufReader::with_capacity(CHUNK_SIZE, reader);
  let mut buffer = Vec::new();
  buf_reader.read_to_end(&mut buffer)?;
  let mut cursor = Cursor::new(buffer);

  // Step 1: Initialize a Zstandard decoder.
  // The decoder decompresses the `.zst` data from the provided cursor.
  let decoder = Decoder::new(&mut cursor)?;

  // Step 2: Create a tar archive reader from the decompressed data.
  let mut archive = Archive::new(decoder);

  // Step 3: Find the `.csv` file with the largest numeric suffix.
  let largest_csv_path = {
    let mut largest_csv_number: usize = 0;
    let mut largest_csv_path: Option<String> = None;

    // First pass: Identify the largest `.csv` file by its numeric suffix.
    for entry in archive.entries()? { //.entries_with_seek()? {
      let path = entry?.path()?.to_string_lossy().to_string();

      if path.ends_with(".csv") {
        // Extract the numeric suffix from the file name.
        if let Some(number) = path
          .rsplit('/')
          .next() // Get the last component of the path (e.g., "0.csv" from "foo/0.csv").
          .and_then(|file| file.strip_suffix(".csv")) // Remove the ".csv" suffix.
          .and_then(|s| s.parse::<usize>().ok())
        // Parse the remaining string as number.
        {
          // Update if a larger number is found.
          if number > largest_csv_number {
            largest_csv_number = number;
            largest_csv_path = Some(path);
          }
        }
      }
    }
    largest_csv_path
  };

  // Reset the cursor to the initial position.
  // TODO: Instead of read the file again, simply seek it. Pending release of `zstd-rs`.
  // https://github.com/gyscos/zstd-rs/pull/310
  cursor.set_position(0);

  let decoder = Decoder::new(cursor)?;
  let mut archive = Archive::new(decoder);

  let mut csv_vec: Vec<String> = Vec::new();

  // Step 4: Iterate over the entries in the tar archive.
  for entry in archive.entries()? {
    // Process each entry, skipping any that result in an error.
    let entry = entry?;

    // Get the entry's path.
    let path = entry.path()?.to_string_lossy().to_string();

    if Some(&path) == largest_csv_path.as_ref() {
      // Read and store the largest `.csv` file content.
      csv_vec.push(entry_to_csv(entry)?);
    }
  }

  Ok(csv_vec)

}

fn entry_to_csv<R: Read>(
  mut entry: Entry<R>,
) -> Result<String, std::io::Error> {
  let mut csv_content = String::with_capacity(entry.size() as usize);
  entry.read_to_string(&mut csv_content)?;
  Ok(csv_content)
}

fn main() {

  println!("This solution works, but it's reading the buffer twice, also decoding the whole encoded content... twice! Find a way to seek. :)");

  let buffer = fs::read("./input.tar.zst").unwrap();
  let cursor = Cursor::new(buffer);

  let result = decompress_latest(cursor).unwrap();
  println!("Result: {:?}", result);

}