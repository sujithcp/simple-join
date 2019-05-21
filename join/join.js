const fs = require('fs');
//const fse = require("fs-extra");
const glob = require("glob");
const crypto = require('crypto');

let args = process.argv.slice(2) || []
const SIZE_LIMIT = 4 * 1000 * 1000 * 1000 - (40 * 1000 * 1000); // 4 GB - 10MB(for index)
let options = {
    "mode": "",
    "checksum": false,
    "split": false
};

let joinMeta = {
    index: {
        files: [],
        fragmented: false,
        nextFragment: null
    },
    srcs: [],
    fragmentNumber: 0,
    out: "",
    baseFileName: ""

}


const HEADER_SIZE = 128;
const PADDING = "\n$$$$$$$$$$$$$$$$$$$$$$$\n"

/**
 * Filter all arguments starting with '-' and set options accordingly
 */
args = args.filter((val, index) => {
    if (/^\-.*$/.test(val)) {
        if (val == '-j' || val == '--join') {
            options.mode = "join"
        } else if (val == '-x' || val == '--extract') {
            options.mode = "extract"
        } else if (val == '-cs' || val == '--checksum') {
            options.checksum = true;
        } else if (val == '-s' || val == '--split') {
            console.log("Ouput will be split into 4GB fragments.");
            options.split = true;
        }
        return false;
    }
    return true;
})

/**
 * Appends index data to file and adds the index content offset
 * @param {String} fileName 
 * @param {Object} index
 */
function writeIndex(fileName, index) {
    if (!fileName) {
        console.log("No filename specified to write index")
        return;
    }
    console.log("\n Writing index")
    let header = Buffer.alloc(HEADER_SIZE, '$');
    fs.appendFileSync(fileName, PADDING);
    header.write("\n$$$$$$$$" + fs.statSync(fileName).size.toString())
    fs.appendFileSync(fileName, JSON.stringify(index));
    fs.appendFileSync(fileName, PADDING)
    fs.appendFileSync(fileName, header.toString("utf8"));

}

/**
 * Creates and initialize a new fragment file
 */
function createNewFragment() {
    joinMeta.index.fragmented = true;

    joinMeta.fragmentNumber++;
    let fragmentNumberString = "000".substr(0, (4 - ("" + joinMeta.fragmentNumber).length)) + joinMeta.fragmentNumber;
    let lastFileFragment = joinMeta.out;
    joinMeta.out = joinMeta.baseFileName + "-frag-" + fragmentNumberString + ".jn";
    try {
        console.log("\nNew file fragment \n", joinMeta.out);
        fs.writeFileSync(joinMeta.out, PADDING, { flag: "wx+" });
        joinMeta.index.nextFragment = joinMeta.out.replace(/^.*\//, "");
        writeIndex(lastFileFragment, joinMeta.index);
    } catch (e) {
        console.log(e.toString());
        process.exit();
    }
    joinMeta.index = {
        files: [],
        fragmented: false,
        nextFragment: null
    };
}

function writeFileToArchive(file, srcRegex = null, fileStat) {
    return new Promise((resolve, reject) => {
        let fileReadStream = fs.createReadStream(file, { autoClose: true, });
        let relativePath = file;
        let sha1 = null;
        let lastSize = fs.statSync(joinMeta.out).size;
        //let data = fs.readFileSync(file);
        if ((lastSize + fileStat.size) > SIZE_LIMIT) {
            if (fileStat.size > SIZE_LIMIT) {
                console.log(`\nError ${file} size (${fileStat.size})  > size limit (${SIZE_LIMIT}), " Bytes !\n`);
                writeIndex(joinMeta.out, joinMeta.index);
                //process.exit(1);
            }
            createNewFragment();
            lastSize = fs.statSync(joinMeta.out).size;
        }
        if (options.checksum) {
            sha1 = crypto.createHash("sha1");
        }
        fileReadStream.on('data', (chunk) => {
            fs.appendFileSync(joinMeta.out, chunk);
            if (options.checksum) {
                sha1.update(chunk);
            }
            if (srcRegex) {
                relativePath = file.replace(srcRegex, "");
            }

        });
        fileReadStream.on("end", () => {
            joinMeta.index.files.push({
                path: relativePath,
                size: fileStat.size,
                start: lastSize,
                length: fs.statSync(joinMeta.out).size - lastSize,
                sha1: (options.checksum) ? sha1.digest('hex') : "NA"

            });
            resolve();
        });
    });
}


async function join(args) {
    joinMeta.srcs = [];
    joinMeta.fragmentNumber = 0;
    // Create output filename if not provided
    joinMeta.out = "./f-join-out-" + new Date().getTime() + ".jn";
    // Extract output filename if specified (last argument)
    console.log(args)
    if (args && (args.length > 1)) {
        joinMeta.out = args[args.length - 1];
        args.splice(args.length - 1, 1);
    }
    joinMeta.srcs = args;
    // Check if extension is .jn
    if (!/.*\.jn/.test(joinMeta.out)) {
        joinMeta.out += ".jn";
        if (!/.*[^\/]+\.jn$/.test(joinMeta.out)) {
            joinMeta.out = joinMeta.out.replace(/.jn$/, "f-join-out-" + new Date().getTime() + ".jn");
        }
    }
    joinMeta.baseFileName = joinMeta.out.replace(/.jn$/, "");
    if (typeof args == "string") {
        joinMeta.srcs = [args];
    }
    try {
        fs.writeFileSync(joinMeta.out, PADDING, { flag: "wx+" });
    } catch (e) {
        console.log(e.toString());
        process.exit();
    }

    for (let dIndex = 0; dIndex < joinMeta.srcs.length; dIndex++) {
        src = joinMeta.srcs[dIndex];
        let count = 0;
        // ../../a/b/c => ../../a/b/ 
        src = src.replace(/\/$/, "");
        let srcBase = src.match(/^.*\//);
        let srcRegex = null;
        if (srcBase) {
            srcRegex = new RegExp(srcBase[0])
        }
        console.log("Searching for files...")
        let files = glob.sync(src + "/**/*", { nodir: true, nosort: true, realpath: true });
        console.log("file list fetched.")
        console.log(files.length);
        for (let i = 0; i < files.length; i++) {
            file = files[i];
            count++;
            let fileStat = fs.statSync(file);
            if (fileStat.isDirectory()) {
                continue;
            }
            process.stdout.write(`\r (${count}/${files.length}) ${fs.realpathSync(file)}                   `)
            fs.appendFileSync(joinMeta.out, PADDING);

            await writeFileToArchive(file, srcRegex, fileStat);

        }

    }
    writeIndex(joinMeta.out, joinMeta.index);
    console.log("Done.")
    console.log(process.uptime())
}


function extract(args) {
    let index = null;
    let isLastFragment = false;
    if (!args) {
        console.log("No file specified");
        process.exit(0);
    }
    let src = args[0];
    let fd = fs.openSync(src, "r")
    let dest = (args.length > 1) ? args[1] : "EXTRACT-" + new Date().getTime();

    while (!isLastFragment) {
        let sizeBuffer = Buffer.alloc(HEADER_SIZE);
        let srcSize = fs.statSync(src).size;
        fs.readSync(fd, sizeBuffer, 0, HEADER_SIZE, srcSize - HEADER_SIZE);
        let indexoffset = sizeBuffer.toString().replace(/\$/g, "").trim();
        indexoffset = parseInt(indexoffset);


        let indexSize = srcSize - indexoffset - HEADER_SIZE - PADDING.length;

        let indexBuffer = Buffer.alloc(indexSize);
        fs.readSync(fd, indexBuffer, 0, indexSize, indexoffset);
        index = JSON.parse(indexBuffer.toString());
        for (let i = 0; i < index.files.length; i++) {

            let file = index.files[i];
            let buffer = Buffer.alloc(file.length);

            fs.readSync(fd, buffer, 0, file.length, file.start);
            let filePath = fs.realpathSync(dest) + "/" + file.path;
            let fileDir = filePath.replace(/\/[^\/]*$/, "");
            if (!fs.existsSync(fileDir)) {
                fs.mkdirSync(fileDir, { recursive: true });
            }
            //fse.ensureFileSync(filePath);
            try {
                fs.writeFileSync(filePath, buffer, { flag: "wx+" });
                process.stdout.write(`\r (${i + 1}/${index.files.length}) - ${filePath}               `);
            } catch (e) {
                console.log(e.toString());
                console.log("Skipping..");
            }
        }
        fs.closeSync(fd, e => console.log(e));
        console.log("\n");
        isLastFragment = true;
        if (index.fragmented) {
            isLastFragment = false;
            let srcBase = fs.realpathSync(src).match(/^.*\//)[0];
            console.log("src Base", srcBase);
            src = srcBase + "/" + index.nextFragment;
            console.log(src);
            fd = fs.openSync(src, "r");
        }
    }

    console.log("Time taken : ", process.uptime());
}


if (options.mode == "join") {
    join(args);
} else if (options.mode == "extract") {
    extract(args);
} else {
    console.log(`
Eg. f-join -j [src] [,[src2], [src3], ..]
    f-join -j pics1 pics2
    f-join -s pics.jn out-dir

Options
-x --extract  : extract the file passed
-j --join   : join the files in the directory passed 
    `)
}
