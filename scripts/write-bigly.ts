import * as fs from 'fs';

const FILE_PATH = './data/digits/value.txt';
const TOTAL_DIGITS = 10_000_000;

function generateDigits() {
  const writeStream = fs.createWriteStream(FILE_PATH);

  console.log(`Writing ${TOTAL_DIGITS} digits to ${FILE_PATH}...`);

  let i = TOTAL_DIGITS;

  function write() {
    let ok = true;

    ok = writeStream.write("literal\n");
    
    // Continue writing until the internal buffer is full or we finish
    while (i > 0 && ok) {
      const digit = Math.floor(Math.random() * 10);
      const data = i === 1 ? `${digit}` : `${digit}\n`;
      
      // write() returns false if the stream needs to drain (buffer is full)
      ok = writeStream.write(data);
      i--;
    }

    if (i > 0) {
      // If we stopped because the buffer is full, wait for 'drain' to continue
      writeStream.once('drain', write);
    } else {
      writeStream.end();
      console.log('✅ Done! File saved.');
    }
  }

  write();
}

generateDigits();