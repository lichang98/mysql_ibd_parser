#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <rapidjson/document.h>
#include <vector>

typedef uint64_t ulint;
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef uint8_t byte;

#define ALLOC(buf, n) buf = (byte *)malloc(n)
#define FREE(buf) free(buf)
#define PAGE_SIZE (1 << 14) // 16KB
#define U16_MAX 0xffff

#define DATA_INT 4       /* integer */
#define DATA_CHAR 29     /* char(n) */
#define DATA_VARCHAR 16  /* varchar(n) */
#define DATA_BIGINT 9    /* bigint */

#define DATA_INT_LEN 4
#define DATA_BIGINT_LEN 8

constexpr u16 FIL_PAGE_SDI = 17853;

constexpr u16 SDI_OFFSET_PAGE_LEVEL = 26;
constexpr u16 SDI_OFFSET_PAGE_DATA = 38;
constexpr u16 SDI_OFFSET_N_RECS = 16;
constexpr u16 SDI_OFFSET_N_NEW_BYTES = 5;
constexpr u16 SDI_OFFSET_REC_TYPE = 3;
constexpr u16 SDI_OFFSET_REC_NEXT = 2;
constexpr u16 SDI_OFFSET_DATA_TYPE_LEN = 4;
constexpr u16 SDI_OFFSET_DATA_TYPE = 0;
constexpr u16 SDI_OFFSET_DATA_ID_LEN = 8;
constexpr u16 SDI_OFFSET_DATA_ID = 4;
constexpr u16 SDI_OFFSET_BLOB_ALLOWED = 4;
constexpr u16 REC_TYPE_ORDINARY = 0;
constexpr u16 REC_TYPE_NODE_PTR = 1;
constexpr u16 REC_TYPE_INF = 2;
constexpr u16 REC_TYPE_SUP = 3;
constexpr u16 REC_OFF_DATA_VARCHAR = 33;

constexpr u16 PAGE_N_HEAP = 4;
constexpr u16 PAGE_HEADER = 38;
constexpr u16 FSEG_HEADER_SIZE = 10;
constexpr u16 REC_NEW_INFO_BITS = 5;
constexpr u16 REC_OLD_INFO_BITS = 6;
constexpr u16 PAGE_NEW_INFIMUM = PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE +
  SDI_OFFSET_N_NEW_BYTES;
constexpr u16 REC_INFO_DELETED_FLAG = 0x20;
constexpr u16 REC_INFO_BITS_SHIFT = 0;
constexpr u16 REC_NEXT = 2;
constexpr u16 REC_OFF_TYPE = 3;
constexpr u16 BTR_EXTERN_LEN = 12;
constexpr u16 REC_OFF_DATA_UNCOMP_LEN = 25;
constexpr u16 REC_OFF_DATA_COMP_LEN = 29;

constexpr u16 FIL_PAGE_INDEX = 17855;

constexpr u16 OFFSET_FILE_PAGE_NO  = 4;
constexpr u16 OFFSET_FILE_SPACE_NO = 34;
constexpr u16 OFFSET_FILE_PAGE_TYPE = 24;
constexpr u16 OFFSET_INDEX_ID = 66;
constexpr u16 OFFSET_PAGE_LEVEL = 64;
constexpr u16 OFFSET_NUM_RECORDS = 54;

constexpr u16 OFFSET_INFIMUM_OFFSET = 97;
constexpr u16 OFFSET_INFIMUM_REC_TYPE = 95;
constexpr u16 OFFSET_INFIMUM_N_OWN = 94;

constexpr u16 OFFSET_SUP_N_OWN = 107;
constexpr u16 OFFSET_SUP_REC_TYPE = 108;
constexpr u16 OFFSET_SUP_NEXT_OFFSET = 110;

constexpr u16 OFFSET_USER_RECS = 120;

struct index_t {
  byte *str_;
  bool m_is_pk_;

  u16 m_page_level_;
  u16 m_num_records_;

  u8  m_inf_n_own_;
  u16 m_inf_rec_type_;
  u16 m_inf_next_offset_;

  u8 m_sup_n_own_;
  u16 m_sup_rec_type_;
  u16 m_sup_next_offset_;

  u32 m_space_no_;
  u32 m_page_no_;
  u64 m_index_id_;

  std::vector<u16> fields_offsets_;
  std::vector<byte *> recs_;
  

  index_t(bool is_pk, u16 page_level, u16 num_records,
          u8 inf_n_own,
          u16 inf_rec_type, u16 inf_next_offset,
          u8 sup_n_own, u8 sup_rec_type,
          u16 sup_next_offset,
          u32 space_no, u32 page_no, u64 index_id): index_t(){
    m_is_pk_ = is_pk;
    m_page_level_ = page_level;
    m_num_records_ = num_records;
    m_inf_n_own_ = inf_n_own;
    m_inf_rec_type_ = inf_rec_type;
    m_inf_next_offset_ = inf_next_offset;
    m_sup_n_own_ = sup_n_own;
    m_sup_rec_type_ = sup_rec_type;
    m_sup_next_offset_ = sup_next_offset;
    m_space_no_ = space_no;
    m_page_no_ = page_no;
    m_index_id_ = index_id;
  }

  index_t(const index_t &other):index_t() {
    m_is_pk_ = other.m_is_pk_;
    m_page_level_ = other.m_page_level_;
    m_num_records_ = other.m_num_records_;
    m_inf_n_own_ = other.m_inf_n_own_;
    m_inf_rec_type_ = other.m_inf_rec_type_;
    m_inf_next_offset_ = other.m_inf_next_offset_;
    m_sup_n_own_ = other.m_sup_n_own_;
    m_sup_rec_type_ = other.m_sup_rec_type_;
    m_sup_next_offset_ = other.m_sup_next_offset_;
    m_space_no_ = other.m_space_no_;
    m_page_no_ = other.m_page_no_;
    m_index_id_ = other.m_index_id_;
  }

  const char *str() {
    assert(str_ != NULL);
    memset(str_, 0, 1024);

    sprintf((char*)str_, "[%s Index space=%u, page=%u,\
level=%hu, num_records=%hu, index_id=%lu]\n\
\tInfimum Record:\n\
\t    rec_type=%hu, next_offset=%hu, n_own=%hhu\n",
            (m_is_pk_ ? "Clust" : "Secondary"),
            m_space_no_, m_page_no_, m_page_level_,
            m_num_records_, m_index_id_,
            m_inf_rec_type_, m_inf_next_offset_, m_inf_n_own_);
    for (u32 i = 0; i < recs_.size(); ++i) {
      sprintf((char *)str_ + strlen((char *)str_), "\t Entry(HEX) [");
      for (u32 j = 1; j < fields_offsets_.size(); ++j) {
        sprintf((char *)str_ + strlen((char *)str_), "0x");
        for (u32 k = 0; k < fields_offsets_[j] - fields_offsets_[j-1]; ++k) {
          sprintf((char *)str_ + strlen((char *)str_),
                  "%02x", *(recs_[i] + fields_offsets_[j - 1] + k));
        }
        sprintf((char *)str_ + strlen((char *)str_), "  ");
      }
      sprintf((char *)str_ + strlen((char *)str_), "]\n");
    }

    for (u32 i = 0; i < recs_.size(); ++i) {
      sprintf((char *)str_ + strlen((char *)str_), "\t Entry(ASCII) [");
      for (u32 j = 1; j < fields_offsets_.size(); ++j) {
        for (u32 k = 0; k < fields_offsets_[j] - fields_offsets_[j-1]; ++k) {
          sprintf((char *)str_ + strlen((char *)str_),
                  "%c", *(recs_[i] + fields_offsets_[j - 1] + k));
        }
        sprintf((char *)str_ + strlen((char *)str_), "  ");
      }
      sprintf((char *)str_ + strlen((char *)str_), "]\n");
    }
    sprintf((char *)str_ + strlen((char *)str_), "\tSupremum Record:\n\
\t    rec_type=%hu, next_offset=%hu, n_own=%hhu",
            m_sup_rec_type_, m_sup_next_offset_, m_sup_n_own_);
    return (const char*)str_;
  }

  ~index_t() {
    FREE(str_);
  }

private:
  index_t():m_is_pk_(false), m_page_level_(0),m_num_records_(0),
            m_inf_n_own_(0),
            m_inf_rec_type_(0), m_inf_next_offset_(0),
            m_sup_n_own_(0), m_sup_rec_type_(0),
            m_sup_next_offset_(0),
            m_space_no_(0), m_page_no_(0), m_index_id_(0) {
    ALLOC(str_, 1024);
    assert(str_ != NULL);
  }
};

struct column_t {
  char m_name_[32];
  u16 m_pos_;
  u16 m_char_len_;

  byte *str_;

  column_t(const char *name, u16 pos, u16 char_len):column_t() {
    strncpy(m_name_, name, strlen(name) + 1);
    m_pos_ = pos;
    m_char_len_ = char_len;
  }

  column_t(const column_t &other):column_t() {
    strncpy(m_name_, other.m_name_, strlen(other.m_name_) + 1);
    m_pos_ = other.m_pos_;
    m_char_len_ = other.m_char_len_;
  }

  const char *str() {
    assert(str_);
    memset(str_, 0, 256);
    sprintf((char *)str_, "[Column name=[%s], pos=%hu, char_len=%hu]\n",
            m_name_,
            m_pos_,
            m_char_len_);
    return (char *)str_;
  }
  
  ~column_t() {
    FREE(str_);
  }

private:
  column_t():m_pos_(0), m_char_len_(0) {
    memset(m_name_, 0, 32);
    ALLOC(str_, 256);
  }
};

struct index_meta_t {
  char m_name_[32];
  struct ele_t {
    column_t *m_col_;
  } m_eles_[64];

  u16 m_ele_cnt_;
  u16 m_id_;

  byte *str_;
  
  index_meta_t(const char *name, u16 id): index_meta_t() {
    strncpy(m_name_, name, strlen(name) + 1);
    m_id_ = id;
  }

  index_meta_t(const index_meta_t &other): index_meta_t() {
    strncpy(m_name_, other.m_name_, strlen(other.m_name_) + 1);
    m_ele_cnt_ = other.m_ele_cnt_;
    m_id_ = other.m_id_;
    for (u32 i = 0; i < m_ele_cnt_; ++i) {
      m_eles_[i].m_col_ = other.m_eles_[i].m_col_;
    }
  }

  void add_ele(column_t *col) {
    m_eles_[m_ele_cnt_++].m_col_ = col;
  }

  const char *str() {
    assert(str_);
    memset(str_, 0, 2048);
    sprintf((char *)str_,
            "[Index id=%hu name=[%s] with %hu ele]\n",
            m_id_, m_name_, m_ele_cnt_);
    for (u16 i = 0; i < m_ele_cnt_; ++i) {
      sprintf((char *)str_ + strlen((char *)str_),
              "\t%s\n", m_eles_[i].m_col_->str());
    }
    return (char *)str_;
  }

  ~index_meta_t() {
    FREE(str_);
  }

private:
  index_meta_t(){
    memset(m_name_, 0, 32);
    m_ele_cnt_ = 0;
    m_id_ = 0;
    ALLOC(str_, 2048);
  }

};

u8 mach_read_from_1(const byte *b) {
  assert(b != NULL);
  return (u8)(b[0]);
}

u16 mach_read_from_2(const byte *b) {
  return (((u16)(b[0])) << 8) | ((u16)(b[1]));
}

u32 mach_read_from_4(const byte *b) {
  return (((u32)b[0]) << 24) | (((u32)b[1]) << 16)
    | (((u32)b[2]) << 8) | (((u32)b[3]));
} 

u64 mach_read_from_8(const byte *b) {
  return (((u64)b[0]) << 56) |(((u64)b[1]) << 48)
    |(((u64)b[2]) << 40) |(((u64)b[3]) << 32)
    |(((u64)b[4]) << 24) |(((u64)b[5]) << 16)
    |(((u64)b[6]) << 8) | ((u64)b[7]);
}

u32 read_page_no(byte *page) {
  return mach_read_from_4(page + OFFSET_FILE_PAGE_NO);
}

u32 read_space_no(byte *page) {
  return mach_read_from_4(page + OFFSET_FILE_SPACE_NO);
}

u16 read_page_type(byte *page) {
  return mach_read_from_2(page + OFFSET_FILE_PAGE_TYPE);
}

u64 read_index_id(byte *page) {
  return mach_read_from_8(page + OFFSET_INDEX_ID);
}

u16 read_page_level(byte *page) {
  return mach_read_from_2(page + OFFSET_PAGE_LEVEL);
}

u16 read_num_records(byte *page) {
  return mach_read_from_2(page + OFFSET_NUM_RECORDS);
}

u16 read_infimum_next_offset(byte *page) {
  return mach_read_from_2(page + OFFSET_INFIMUM_OFFSET);
}

u16 read_infimum_rec_type(byte *page) {
  return mach_read_from_2(page + OFFSET_INFIMUM_REC_TYPE) & 0x7;
}

u8 read_infimum_n_own(byte *page) {
  return mach_read_from_1(page + OFFSET_INFIMUM_N_OWN) & 0xf;
}

u8 read_sup_n_own(byte *page) {
  return mach_read_from_1(page + OFFSET_SUP_N_OWN) & 0xf;
}

u16 read_sup_rec_type(byte *page) {
  return mach_read_from_2(page + OFFSET_SUP_REC_TYPE) & 0x7;
}

u16 read_sup_next_offset(byte *page) {
  return mach_read_from_2(page + OFFSET_SUP_NEXT_OFFSET);
}

u16 read_rec_next_offset(byte *rec) {
  return mach_read_from_2(rec - REC_NEXT);
}

FILE *open_file(char *fname) {
  FILE *fp = fopen(fname, "rb");
  return fp;
}

bool next_page(FILE *fp, byte *buf) {
  size_t ret = fread(buf, 1, PAGE_SIZE, fp);
  bool eof = feof(fp);
  assert(ret == PAGE_SIZE || eof);
  return !eof;
}

void read_page_with_no(FILE *fp, u32 page_no, byte *buf) {
  u64 pos = ftell(fp);
  fseek(fp, page_no * PAGE_SIZE, SEEK_SET);
  size_t ret = fread(buf, 1, PAGE_SIZE, fp);
  bool eof = feof(fp);
  assert(ret == PAGE_SIZE && !eof);
  fseek(fp, pos, SEEK_SET);
}

void load_index_fields(byte *page, u16 bg_offset,
                       index_meta_t &index_meta,
                       std::vector<byte *> &recs,
                       std::vector<u16> &fields_offsets) {
  u16 offset = bg_offset;
  fields_offsets.emplace_back(0);
  for (u32 i = 0; i < index_meta.m_ele_cnt_; ++i) {
    fields_offsets.emplace_back(index_meta.m_eles_[i].m_col_->m_char_len_
                                + fields_offsets.back());
  }

  offset += OFFSET_INFIMUM_OFFSET + 2;
  while (offset != (OFFSET_SUP_NEXT_OFFSET + 2)) {
    recs.emplace_back(page + offset);
    offset += read_rec_next_offset(page + offset);
  }
}



u16 sdi_read_page_level(byte *page) {
  return mach_read_from_2(page + SDI_OFFSET_PAGE_DATA +
                          SDI_OFFSET_PAGE_LEVEL);
}

u16 sdi_read_n_recs(byte *page) {
  return mach_read_from_2(page + SDI_OFFSET_PAGE_DATA +
                          SDI_OFFSET_N_RECS);
}

u8 sdi_read_rec_type(byte *page) {
  return mach_read_from_1(page + PAGE_NEW_INFIMUM -
                          SDI_OFFSET_REC_TYPE);
}

u16 sdi_read_next_rec_offset(byte *page) {
  return mach_read_from_2(page + PAGE_NEW_INFIMUM -
                          SDI_OFFSET_REC_NEXT);
}

u32 sdi_read_child_page_no(byte *page, u16 next_rec_offset) {
  return mach_read_from_4(page + PAGE_NEW_INFIMUM + 
                          next_rec_offset +
                          SDI_OFFSET_DATA_TYPE_LEN +
                          SDI_OFFSET_DATA_ID_LEN);
}

u8 sdi_rec_type(byte *rec) {
  u8 rec_type = *(rec - REC_OFF_TYPE);
  return rec_type & 0x7;
}

bool page_is_comp(byte *page) {
  return mach_read_from_2(page + PAGE_HEADER + PAGE_N_HEAP) & 0x8000;
}

bool rec_is_deleted(byte *rec, bool is_comp) {
  if (is_comp) {
    return (mach_read_from_1(rec - REC_NEW_INFO_BITS)
            & REC_INFO_DELETED_FLAG) >> REC_INFO_BITS_SHIFT;
  } else {
    return (mach_read_from_1(rec - REC_OLD_INFO_BITS)
            & REC_INFO_DELETED_FLAG) >> REC_INFO_BITS_SHIFT;
  }
}

void parse_rec_data(byte *rec, byte *&sdi_data, u64 &data_len, u64 &id) {
  id = mach_read_from_8(rec + SDI_OFFSET_DATA_ID);
  printf("[INFO] parse sdi rec with id=%lu\n", id);
  byte rec_data_len_partial = *(rec - SDI_OFFSET_N_NEW_BYTES - 1);
  u64 rec_data_len;
  bool is_rec_data_external = false;
  u32 sdi_uncomp_len = mach_read_from_4(rec + REC_OFF_DATA_UNCOMP_LEN);
  u32 sdi_comp_len = mach_read_from_4(rec + REC_OFF_DATA_COMP_LEN);
  printf("[INFO] sdi_uncomp_len=%u, sdi_comp_len=%u\n",
         sdi_uncomp_len, sdi_comp_len);
  if (rec_data_len_partial & 0x80) {
    u32 rec_data_in_page_len = 0;
    rec_data_in_page_len = (rec_data_len_partial &0x3f) << 8;
    if (rec_data_len_partial & 0x40) {
      is_rec_data_external = true;
      rec_data_len = mach_read_from_8(rec + REC_OFF_DATA_VARCHAR +
                                      rec_data_in_page_len + BTR_EXTERN_LEN);
      rec_data_len += rec_data_in_page_len;
    } else {
      rec_data_len = *(rec - SDI_OFFSET_N_NEW_BYTES - 2);
      rec_data_len += rec_data_in_page_len;
    }
  } else {
    rec_data_len = rec_data_len_partial;
  }
  byte *str;
  ALLOC(str, rec_data_len + 1);
  memset(str, 0, rec_data_len + 1);
  byte *rec_data_origin = rec + REC_OFF_DATA_VARCHAR;
  if (!is_rec_data_external) {
    memcpy(str, rec_data_origin, rec_data_len);
    data_len = rec_data_len;
    sdi_data = str;
    byte *uncomp_sdi;
    ALLOC(uncomp_sdi, sdi_uncomp_len + 1);
    memset(uncomp_sdi, 0, sdi_uncomp_len + 1);
    u64 dest_len = sdi_uncomp_len + 1;
    int ret = uncompress(uncomp_sdi, &dest_len, str, sdi_comp_len);
    assert(ret == Z_OK);
    data_len = sdi_uncomp_len + 1;
    sdi_data = uncomp_sdi;
    FREE(str);
  }
}

byte *sdi_next_rec(byte *page, byte *curr_rec, bool is_comp) {
  u64 next_rec_offset = mach_read_from_2(curr_rec - REC_NEXT);
  if (is_comp) {
    if (next_rec_offset !=0 ){
      next_rec_offset =
        ((u64)(curr_rec - page + next_rec_offset)) & (PAGE_SIZE - 1);
    }
  }
  printf("[INFO] sdi next rec offset=%lu\n", next_rec_offset);
  assert(next_rec_offset != 0);
  return page + next_rec_offset;
}

void extract_sdi_info (byte *sdi_str, std::vector<index_meta_t> &indexes,
                       std::vector<column_t> &columns) {

  rapidjson::Document doc;
  doc.Parse((const char*)sdi_str);
  if (strcmp(doc["dd_object_type"].GetString(), "Table") != 0) {
    return;
  }
  const rapidjson::Value &cols = doc["dd_object"]["columns"];
  printf("[INFO] Got %d cols\n", cols.Size());
  for(rapidjson::SizeType i = 0; i < cols.Size(); ++i) {
    u16 char_length = (u16)cols[i]["char_length"].GetInt();
    if (cols[i]["type"].GetInt() == DATA_INT) {
      char_length = DATA_INT_LEN;
    } else if (cols[i]["type"].GetInt() == DATA_BIGINT &&
               strcmp(cols[i]["name"].GetString(), "DB_ROLL_PTR") != 0) {
      char_length = DATA_BIGINT_LEN;
    } else if (cols[i]["type"].GetInt() == DATA_CHAR ||
               cols[i]["type"].GetInt() == DATA_VARCHAR) {
      char char_size[10] = {'\0'};
      strncpy(char_size, strchr(cols[i]["column_type_utf8"].GetString(), '(') + 1,
              strlen(strchr(cols[i]["column_type_utf8"].GetString(), '(') + 1));
      char_size[strlen(char_size) - 1] = '\0';
      char_length = atoi(char_size);
    }
    columns.emplace_back(cols[i]["name"].GetString(),
                                  (u16)cols[i]["ordinal_position"].GetInt(),
                                  char_length);
    printf("[INFO] Got %s\n", columns.back().str());
  }

  const rapidjson::Value &inds = doc["dd_object"]["indexes"];
  printf("[INFO] Got %d indexes\n", inds.Size());
  for(rapidjson::SizeType i = 0; i < inds.Size(); ++i) {
    const char *se_priv = inds[i]["se_private_data"].GetString();
    char *idx_id_pos = strchr((char *)se_priv, ';');
    char idx_id[10] = {'\0'};
    strncpy(idx_id, se_priv + 3, idx_id_pos - se_priv - 3);
    indexes.emplace_back(inds[i]["name"].GetString(), atoi(idx_id));
    const rapidjson::Value &ele_cols = inds[i]["elements"];
    for (rapidjson::SizeType j = 0; j < ele_cols.Size(); ++j) {
      u32 col_pos = ele_cols[j]["column_opx"].GetInt();
      indexes.back().add_ele(&columns[col_pos]);
    }
    printf("[INFO] Got %s\n", indexes.back().str());
  }
}

void sdi(FILE *fp, byte *buf, std::vector<index_meta_t> &index_metas,
         std::vector<column_t> &cols) {
  u16 page_level = sdi_read_page_level(buf);
  assert(page_level != U16_MAX);
  u16 n_recs = sdi_read_n_recs(buf);
  printf("[INFO] sdi: n_recs=%hu\n", n_recs);
  if (n_recs == 0) { return; }

  while(page_level != 0 && page_level != U16_MAX) {
    u8 rec_type = sdi_read_rec_type(buf);
    assert(rec_type == REC_TYPE_INF);
    u16 next_offset = sdi_read_next_rec_offset(buf);
    u32 child_page_no = sdi_read_child_page_no(buf, next_offset);
    assert(child_page_no >= SDI_OFFSET_BLOB_ALLOWED);
    printf("[INFO] sdi read child page no=%u\n", child_page_no);
    read_page_with_no(fp, child_page_no, buf);
    page_level = sdi_read_page_level(buf);
  }

  u16 next_offset = sdi_read_next_rec_offset(buf);
  printf("[INFO] next_offset=%hu\n", next_offset);
  byte *curr_rec = buf + PAGE_NEW_INFIMUM + next_offset;
  bool is_comp = page_is_comp(buf);
  bool is_deleted = rec_is_deleted(curr_rec, is_comp);
  printf("[INFO] get first rec, is_comp=%u, is_deleted=%u\n",
         is_comp, is_deleted);
  if (is_deleted) {
    curr_rec = sdi_next_rec(buf, curr_rec, is_comp);
  }

  u64 sdi_id;
  u64 sdi_data_len = 0;
  byte *sdi_data = NULL;

  while (curr_rec != NULL &&
         sdi_rec_type(curr_rec) != REC_TYPE_SUP) {
    printf("[INFO] sdi rec type=%hhu\n", sdi_rec_type(curr_rec));
    parse_rec_data(curr_rec, sdi_data, sdi_data_len, sdi_id);
    extract_sdi_info(sdi_data, index_metas, cols);
    FREE(sdi_data);
    sdi_data = NULL;
    curr_rec = sdi_next_rec(buf, curr_rec, is_comp);
  }
}

void close_file(FILE *fp) {
  fclose(fp);
}

void read_pages(char *fname) {
  FILE *fp = open_file(fname);
  byte *buf;
  ALLOC(buf, PAGE_SIZE);

  u8 n_index = 0;
  std::vector<index_t> indexes;
  std::vector<byte *> index_pages;
  std::vector<index_meta_t> index_metas;
  std::vector<column_t> cols;

  while(next_page(fp, buf)) {
    u16 page_type = read_page_type(buf);

    switch(page_type) {
    case FIL_PAGE_INDEX:
      {
        indexes.emplace_back(n_index == 0, read_page_level(buf),
                             read_num_records(buf),
                             read_infimum_n_own(buf),
                             read_infimum_rec_type(buf),
                             read_infimum_next_offset(buf),
                             read_sup_n_own(buf),
                             read_sup_rec_type(buf),
                             read_sup_next_offset(buf),
                             read_space_no(buf), read_page_no(buf),
                             read_index_id(buf));
        ++n_index;
        byte *page_clone;
        ALLOC(page_clone, PAGE_SIZE);
        memcpy(page_clone, buf, PAGE_SIZE);
        index_pages.emplace_back(page_clone);
        printf("[INFO] >Got index:\n%s\n", indexes.back().str());
      }
      break;
    case FIL_PAGE_SDI:
      sdi(fp, buf, index_metas, cols);
      break;
    default:
      break;
    }
  }

  /* Load rec fields according to sdi index info */
  for (u32 i = 0; i < indexes.size(); ++i){
    for (index_meta_t &ind_meta : index_metas) {
      if (ind_meta.m_id_ == indexes[i].m_index_id_) {
        load_index_fields(index_pages[i],
                          read_infimum_next_offset(index_pages[i]),
                          ind_meta,
                          indexes[i].recs_,
                          indexes[i].fields_offsets_);
        break;
      }
    }
  }

  for (u32 i = 0; i < indexes.size(); ++i) {
    printf("[INFO] >Parse index:\n%s\n", indexes[i].str());
  }

  FREE(buf);
  for (byte *ind_page : index_pages) {
    FREE(ind_page);
  }
  close_file(fp);
}


int main(int argc, char *argv[]) {
  read_pages(argv[1]);
  return 0;
}
