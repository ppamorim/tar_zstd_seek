use std::{
  fs,
  io::{BufReader, Cursor, Read},
};

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

fn decompress<R: Read>(
  reader: R,
) -> Result<Vec<String>, DecompressError> {

  let mut decoder = Decoder::new(reader)?;
  let mut buffer = Vec::new();
  _ = std::io::copy(&mut decoder, &mut buffer);

  let mut cursor = Cursor::new(&buffer);
  let mut archive = Archive::new(&mut cursor);

  let mut csv_contents = Vec::new();

  for entry in archive.entries_with_seek()? {
    let entry = entry?;
    let path = entry.path()?.to_string_lossy().to_string();
    if path.starts_with("transcriptions/") && path.ends_with(".csv") {
      csv_contents.push(entry_to_csv(entry)?);
    }
  }

  Ok(csv_contents)
}

fn decompress_latest<R: Read>(
  reader: R,
) -> Result<Vec<String>, DecompressError> {

  let mut decoder = Decoder::new(reader)?;
  let mut buffer = Vec::new();
  _ = std::io::copy(&mut decoder, &mut buffer);

  let mut cursor = Cursor::new(&buffer);
  let mut archive = Archive::new(&mut cursor);

  // Find the `.csv` file with the largest numeric suffix
  let largest_csv_path = archive
    .entries_with_seek()?
    .filter_map(|entry| {
      let entry = entry.ok()?;
      let path = entry.path().ok()?.to_string_lossy().to_string();

      if path.starts_with("transcriptions/") && path.ends_with(".csv") {
        path
          .rsplit('/')
          .next()
          .and_then(|file| file.strip_suffix(".csv"))
          .and_then(|num| num.parse::<usize>().ok())
          .map(|num| (num, path))
      } else {
        None
      }
    })
    .max_by_key(|(num, _)| *num)
    .map(|(_, path)| path);

  // Reset cursor and process entries again to read the largest `.csv` file
  cursor.set_position(0);
  archive = Archive::new(&mut cursor);

  let mut csv_contents = Vec::new();

  if let Some(ref target_path) = largest_csv_path {
    for entry in archive.entries_with_seek()? {
      let entry = entry?;
      let path = entry.path()?.to_string_lossy().to_string();

      if path == *target_path {
        csv_contents.push(entry_to_csv(entry)?);
        break;
      }
    }
  }

  Ok(csv_contents)
}

fn entry_to_csv<R: Read>(
  mut entry: Entry<R>,
) -> Result<String, std::io::Error> {
  let mut csv_content = String::with_capacity(entry.size() as usize);
  entry.read_to_string(&mut csv_content)?;
  Ok(csv_content)
}

fn main() {
  println!("This solution works, but it's slower than trivially reading all files");

  let buffer = fs::read("./input.tar.zst").unwrap();

  let cursor = Cursor::new(&buffer);
  let result = decompress(cursor).unwrap();
  println!("decompress: {:?}", result);

  let cursor = Cursor::new(buffer);
  let result = decompress_latest(cursor).unwrap();
  println!("decompress_latest: {:?}", result);
}
