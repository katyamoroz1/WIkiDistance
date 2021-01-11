#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <boost/process.hpp>
#include <boost/process/system.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/process/io.hpp>
#include <boost/filesystem.hpp>
#include <set>

#define el_in_one_file 60
#define max_child 50

namespace bp = boost::process;
namespace bf = boost::filesystem;

enum args {
    OPERATION = 1,
    PATH_TO_SCRIPT = 2,
    SRC_FILE = 3,
    DST_FILE = 4,
    DEPTH = 5
};

void find_links(const std::string &url, const std::string &file) {
    std::string command = "python3 temp.py " + url + " " + file + " 2>&-";
    system(command.c_str());
}

void sort_one_file(const std::string &file, int64_t &all_strings) {
    std::ifstream in(file);
    std::string line;
    std::set<std::string> s;
    while (getline(in, line)) {
        s.insert(line);
    }
    in.close();
    std::ofstream out(file, std::ios_base::trunc);
    for (auto &string : s) {
        out << string << std::endl;
    }
    out.close();
    all_strings += s.size();
}

void merge_files(std::vector<std::string> &files, const std::string &out_file, int64_t all_strings) {
    std::vector<std::ifstream> streams(files.size());
    std::ofstream out(out_file, std::ios_base::trunc);
    std::set<std::pair<std::string, int64_t>> s;
    for (int64_t i = 0; i < files.size(); i++) {
        streams[i].open(files[i]);
        std::string line;
        getline(streams[i], line);
        s.insert({line, i});
    }
    for (int64_t k = 0; k < all_strings; k++) {
        auto el = *s.begin();
        if (!streams[el.second].eof()) {
            std::string line;
            s.erase(s.begin());
            out << el.first << std::endl;
            getline(streams[el.second], line);
            if (!streams[el.second].eof()) {
                s.insert({line, el.second});
            }
        }
    }
    for (auto &stream : streams) {
        stream.close();
    }
    out.close();
}

void merge_files_without_sorting(std::vector<std::string> &files, const std::string &out_file_name) {
    std::ofstream out(out_file_name, std::ios_base::trunc);
    for (auto &file : files) {
        std::string line;
        std::ifstream in(file);
        while (getline(in, line)) {
            out << line << std::endl;
        }
        in.close();
        bf::remove(file);
    }
    out.close();
}

int64_t split_file(const std::string &file, std::vector<std::string> &input_split_files) {
    std::string line;
    std::ifstream in(file);
    int64_t count = 0;
    int64_t count_of_file = 0;
    bf::path temp = bf::unique_path();
    const std::string tempstr = temp.string();
    input_split_files.push_back(tempstr);
    std::ofstream out(tempstr);
    while (getline(in, line)) {
        out << line << std::endl;
        count++;
        if (count == el_in_one_file) {
            count = 0;
            out.close();
            count_of_file++;
            temp = bf::unique_path();
            const std::string tempstr1 = temp.string();
            input_split_files.push_back(tempstr1);
            out.open(tempstr1);
        }
    }
    in.close();
    out.close();
    return count_of_file;
}

int main(int argc, char *argv[]) {
    if (argc != 6) {
        std::cout << "incorrect input" << std::endl;
        return 1;
    }
    if (!strcmp(argv[OPERATION], "map")) {
        std::vector<std::string> input_split_files;
        int64_t n = split_file(argv[SRC_FILE], input_split_files);
        std::vector<bp::child> threads;
        std::vector<std::string> output_split_files;
        for (int64_t i = 0; i <= n; i++) {
            bf::path temp = bf::unique_path();
            const std::string tempstr = temp.string();
            output_split_files.push_back(tempstr);
            threads.emplace_back(argv[PATH_TO_SCRIPT], bp::std_out > tempstr, bp::std_err > stderr,
                                 bp::std_in < input_split_files[i]);

        }
        for (auto &child : threads) {
            std::error_code error_code;
            child.wait(error_code);
            if (error_code.value() != 0) {
                std::cout << "Error wile mapping" << std::endl;
                return 1;
            }
        }
        for (auto &file : input_split_files) {
            bf::remove(file);
        }
        const std::string &out_file_name = argv[DST_FILE];
        std::ofstream out(out_file_name, std::ios_base::trunc);
        merge_files_without_sorting(output_split_files, out_file_name);
    } else if (!strcmp(argv[OPERATION], "reduce")) {
        std::string start_file = argv[SRC_FILE];
        std::vector<std::string> depth_files;
        for (int64_t depth = 0; depth < std::stoi(argv[DEPTH]); depth++) {
            std::vector<std::string> files;
            int64_t all_strings = 0;
            int64_t number_of_files = split_file(start_file, files);
            for (auto &file : files) {
                sort_one_file(file, all_strings);
            }
            bf::path temp = bf::unique_path();
            const std::string out_file = temp.string();
            merge_files(files, out_file, all_strings);
            for (auto &file : files) {
                bf::remove(file);
            }
            std::ifstream in(out_file);
            std::string curr_line;
            std::string prev_line;
            getline(in, curr_line);
            prev_line = curr_line;
            std::vector<std::string> file_names;
            bf::path t = bf::unique_path();
            const std::string tempstr = t.string();
            std::vector<bp::child> threads;
            std::ofstream out(tempstr);
            file_names.push_back(tempstr);
            out << prev_line << std::endl;
            while (getline(in, curr_line)) {
                if (curr_line == prev_line) {
                    out << curr_line << std::endl;
                } else {
                    prev_line = curr_line;
                    out.close();
                    temp = bf::unique_path();
                    const std::string tempstr1 = temp.string();
                    out.open(tempstr1);
                    out << curr_line << std::endl;
                    file_names.push_back(tempstr1);
                }
            }
            in.close();
            out.close();
            std::vector<std::string> output_split_files;
            for (int64_t i = 0; i < file_names.size(); i++) {
                bf::path temp1 = bf::unique_path();
                const std::string tempstr1 = temp1.string();
                output_split_files.push_back(tempstr1);
                threads.emplace_back(argv[PATH_TO_SCRIPT], bp::std_out > tempstr1, bp::std_err > stderr,
                                     bp::std_in < file_names[i]);
                if (threads.size() == max_child) {
                    for (auto &child : threads) {
                        std::error_code error_code;
                        child.wait(error_code);
                        if (error_code.value() != 0) {
                            std::cout << "Error while reduce" << std::endl;
                            return 1;
                        }
                    }
                    threads.clear();
                }
            }
            for (auto &child : threads) {
                std::error_code error_code;
                child.wait(error_code);
                if (error_code.value() != 0) {
                    std::cout << "Error while reduce" << std::endl;
                    return 1;
                }
            }
            merge_files_without_sorting(output_split_files, start_file);
            bf::remove(out_file);
            for (auto &file : file_names) {
                bf::remove(file);
            }
            for (auto &file : output_split_files) {
                bf::remove(file);
            }
            in.open(start_file);
            std::string line;
            std::vector<std::string> files_with_links;
            while (getline(in, line)) {
                bf::path f = bf::unique_path();
                const std::string file_name = f.string();
                files_with_links.push_back(file_name);
                std::string::size_type pos = line.find('\t');
                std::string value = line.substr(pos + 1);
                line.resize(pos);
                if (value == "0") {
                    find_links(line, file_name);
                    out.open(file_name, std::ios_base::app);
                    out << line << '\t' << "1" << std::endl;
                    out.close();
                }
            }
            in.close();
            bf::path f = bf::unique_path();
            const std::string depth_file = f.string();
            depth_files.push_back(depth_file);
            merge_files_without_sorting(files_with_links, depth_file);
            files_with_links.clear();
            start_file = depth_file;
        }
        std::ifstream in(start_file);
        std::ofstream out(argv[DST_FILE]);
        std::string line;
        while (getline(in, line)) {
            std::string::size_type pos = line.find('\t');
            line.resize(pos);
            out << line << '\t' << "" << std::endl;
        }
        in.close();
        out.close();
        bf::remove(start_file);
        for (auto &d : depth_files) {
            bf::remove(d);
        }
    } else {
        std::cout << "incorrect input" << std::endl;
        return 1;
    }
    return 0;
}
