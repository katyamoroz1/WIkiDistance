#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    std::string line;
    while (getline(std::cin, line)) {
        std::string::size_type pos = line.find('\t');
        line.resize(pos);
        std::cout << line << '\t' << std::to_string(0) << std::endl;
    }
    return 0;
}