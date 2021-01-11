#include <iostream>
#include <fstream>
#include <boost/process.hpp>
#include <boost/filesystem.hpp>

namespace bf = boost::filesystem;

int main(int argc, char* argv[]) {
    bf::path temp = bf::unique_path();
    const std::string& tempstr = temp.native();
    std::string file = argv[1];
    std::string command = "python3 temp.py https://ru.wikipedia.org/wiki/Linux " + tempstr;
    system(command.c_str());
    std::srand(unsigned(std::time(0)));
    int number_of_url = std::rand() % 280;
    std::ifstream in(tempstr);
    std::string line;
    for (int i = 0; i < number_of_url; i++) {
        getline(in, line);
    }
    std::string::size_type pos = line.find('\t');
    line.resize(pos);
    std::string command1 = "python3 temp1.py " + line + " " + file;
    system(command1.c_str());
    bf::remove(tempstr);
    return 0;
}