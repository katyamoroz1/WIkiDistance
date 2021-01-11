#include <iostream>

int main(int argc, char *argv[]) {
    std::string line;
    getline(std::cin, line);
    std::string::size_type pos = line.find('\t');
    std::string value = line.substr(pos + 1);
    line.resize(pos);
    if (value == "1") {
        std::cout << line << '\t' << "1" << std::endl;
        return 0;
    }
    std::string line1;
    while (getline(std::cin, line1)) {
        if (line1.substr(pos + 1) == "1") {
            std::cout << line << '\t' << "1" << std::endl;
            return 0;
        }
    }
    std::cout << line << '\t' << "0" << std::endl;
    return 0;
}