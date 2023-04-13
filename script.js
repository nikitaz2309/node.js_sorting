const fs = require('fs');
const readline = require('readline');

async function splitFile(inputFile, maxLines = 1000000) {
  const inputStream = fs.createReadStream(inputFile, 'utf8');
  const reader = readline.createInterface({ input: inputStream });
  let lineCount = 0;
  let fileCount = 0;
  let outputStream = null;

  for await (const line of reader) {
    if (lineCount % maxLines === 0) {
      if (outputStream) {
        outputStream.close();
      }
      outputStream = fs.createWriteStream(`${inputFile}-part-${fileCount}`, 'utf8');
      fileCount++;
    }
    outputStream.write(line + '\n');
    lineCount++;
  }

  if (outputStream) {
    outputStream.close();
  }
  inputStream.close();
}

async function mergeFiles(inputFiles, outputFile) {
  const inputStreams = inputFiles.map((file) => fs.createReadStream(file, 'utf8'));
  const readers = inputStreams.map((inputStream) => readline.createInterface({ input: inputStream }));
  const lines = [];

  for (const reader of readers) {
    const { value, done } = await reader[Symbol.asyncIterator]().next();
    if (!done) {
      lines.push({
        value,
        reader
      });
    }
  }

  const outputStream = fs.createWriteStream(outputFile, 'utf8');

  while (lines.length > 0) {
    lines.sort((a, b) => a.value.localeCompare(b.value));
    const { value, reader } = lines.shift();
    outputStream.write(value + '\n', 'utf8');

    const { value: nextValue, done } = await reader[Symbol.asyncIterator]().next();
    if (!done) {
      lines.push({
        value: nextValue,
        reader
      });
    }
  }

  outputStream.close();
  inputStreams.forEach((inputStream) => inputStream.close());
}

function deleteTempFiles(inputFiles) {
  inputFiles.forEach((file) => fs.unlinkSync(file));
}

async function sortBigFile(inputFile, outputFile) {
  await splitFile(inputFile);
  const inputFiles = fs.readdirSync('./')
    .filter((file) => file.startsWith(inputFile) && file !== inputFile)
    .sort()
    .map((file) => `./${file}`);

  await mergeFiles(inputFiles, outputFile);
  deleteTempFiles(inputFiles);
}