#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/util/future.h>
#include <parquet/arrow/reader.h>
#include <iostream>
#include <deque>

int main() {
    // Initialize Arrow and Parquet libraries
    arrow::Status status;

    // Open the Parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile = arrow::io::ReadableFile::Open("test.parquet").ValueOrDie();

    parquet::arrow::FileReaderBuilder frb;
    parquet::ArrowReaderProperties properties;
    properties.set_use_threads(true);
    frb.properties(properties);

    // Create a Parquet file reader
    status = frb.Open(infile);
    if (!status.ok()) {
        std::cerr << "Failed to open file reader: " << status.ToString() << std::endl;
        return -1;
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    status = frb.Build(&reader);
    if (!status.ok()) {
        std::cerr << "Failed to create Parquet file reader: " << status.ToString() << std::endl;
        return -1;
    }

    std::vector<int> rgs;
    for (int i = 0; i < reader->num_row_groups(); ++i) {
        rgs.push_back(i);
    }
    std::shared_ptr<arrow::Schema> schema;
    status = reader->GetSchema(&schema);
    if (!status.ok()) {
        std::cerr << "Failed to get schema: " << status.ToString() << std::endl;
        return -1;
    }

    std::vector<int> cols;
    for (int i = 0; i < schema->num_fields(); ++i) {
        cols.push_back(i);
    }

    auto sharedReader = std::shared_ptr<parquet::arrow::FileReader>(std::move(reader));
    while (true) {
        auto generator = sharedReader->GetRecordBatchGenerator(sharedReader,
            rgs, cols);
        if (!generator.ok()) {
            std::cerr << "Failed to get generator: " << generator.status().ToString() << std::endl;
            return -1;
        }

        auto gen = generator.MoveValueUnsafe();
        std::deque<::arrow::Future<std::shared_ptr<::arrow::RecordBatch>>> rbs;
        while (true) {
            while (rbs.size() < 2) {
                rbs.push_back(gen());
            }
            auto next = std::move(rbs.front());
            rbs.pop_front();
            auto nextR = next.MoveResult().ValueOrDie();
            if (!nextR) {
                break;
            }

            std::cout << nextR->num_rows() << std::endl;
        }
    }

    // Print the table schema
    // std::cout << "Schema: " << table->schema()->ToString() << std::endl;
    // std::cout << table->num_rows() << std::endl;

    return 0;
}