const fs = require('fs');
const path = require('path');

const [csv, table] = process.argv.slice(2);

// Read the CSV file
fs.readFile(csv, 'utf8', (err, data) => {
    if (err) {
        console.error("Error reading the file:", err);
        return;
    }

    // Split into lines and remove empty lines
    const lines = data.trim().split('\n');
    
    // Extract headers (first row)
    const headers = lines[0].split(',');
    
    // Initialize arrays for each column
    const columns = headers.map(() => []);

    // Loop through data rows (starting at index 1 to skip headers)
    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',');
        values.forEach((val, index) => {
            if (columns[index]) {
                columns[index].push(val.trim());
            }
        });
    }

    fs.mkdirSync(`./data/${table}`, { recursive: true });

    // Write each column to its own .txt file
    headers.forEach((header, index) => {
        const fileName = `${header.toLowerCase()}.txt`;
        let content = "";
        
        if (fileName === "make.txt") {
            content += "dictionary\n";
            const uniques = [...new Set(columns[index])];
            content += JSON.stringify(uniques) + "\n";
            for (const v of columns[index]) {
                content += uniques.indexOf(v) + "\n";
            }
        } else {
            content += "literal\n";
            content += columns[index].join('\n');
        }
        
        fs.writeFile(`./data/${table}/${fileName}`, content, (err) => {
            if (err) {
                console.error(`Error writing ${fileName}:`, err);
            } else {
                console.log(`Successfully created: ${fileName}`);
            }
        });
    });
});