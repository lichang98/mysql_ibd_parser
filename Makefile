main: main.cc
	g++ main.cc -I /mysql/extra/zlib/ -I /mysql/extra/rapidjson/include -L /mysql/archive_output_directory -lz -lzstd -o main
