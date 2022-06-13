# mysql_ibd_parser

For parsing indexes and records from .ibd file

## Brief Introduction

Newer version mysql(v8.0+) saves data dictionary and records in one file while the older version ones save them separately into .frm, .MYD and .MYI.
Mysql provides **ibd2sdi**, which can be used to extract dictionary info, but not contains records.
The fields of one row are saved in binary, retriving single field is not possible without table info.
Since SDI is stored in .ibd, we can use information in it to separate fields from binary.

## Usage

Replace the path in Makefile with your own.
The target table should use InnoDB engine, and using **file-per-table** mode.
The program now only support table with column type int, char and varchar, you can 
modify codes from line 523 to 537 to support more types.

`make`

`./main <path to .ibd file>`

## Sample

**Table**
----
<img width="401" alt="image" src="https://user-images.githubusercontent.com/27817383/173285859-845cfb18-0662-4d37-913d-9774ae268f04.png">

**Output**
----
<img width="450" alt="image" src="https://user-images.githubusercontent.com/27817383/173286231-1ce7082d-d72e-4921-b5fc-a1cc3aadc862.png">
<img width="883" alt="image" src="https://user-images.githubusercontent.com/27817383/173286284-5568e04a-e9a0-4858-ba6a-6c5610e42131.png">

## Acknowledgements

- [https://blog.jcole.us/innodb/](https://blog.jcole.us/innodb/)
- mysql ibd2sdi

