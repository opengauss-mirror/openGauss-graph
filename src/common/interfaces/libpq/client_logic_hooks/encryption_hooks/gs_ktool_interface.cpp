/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * gs_ktool_interface.cpp
 *
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\gs_ktool_interface.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "gs_ktool_interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <openssl/rand.h>
#include "encrypt_decrypt.h"
#include "aead_aes_hamc_enc_key.h"
#include "gs_ktool/kt_interface.h"
#include "cmk_cache_lru.h"

static CmkCacheList *cmk_cache_list = NULL;

extern bool kt_check_algorithm_type(CmkAlgorithm algo_type)
{
    if (algo_type == CmkAlgorithm::AES_256_CBC) {
        return true;
    } else {
        printf("ERROR(CLIENT): Invalid algorithm, keys generated by gs_ktool are only used for AES_256_CBC.\n");
    }

    return false;
}

bool kt_atoi(const char *cmk_id_str, unsigned int *cmk_id)
{
    const char *key_path_tag = "gs_ktool/";
    char tmp_str[MAX_KEYPATH_LEN] = {0};
    int tmp_pos = 0;
    bool has_invalid_char = false;

    if (cmk_id_str == NULL || strlen(cmk_id_str) <= strlen(key_path_tag)) {
        printf("ERROR(CLIENT): Invalid key path, it should be like \"%s1\".\n", key_path_tag);
        return false;
    }

    for (size_t i = 0; i < strlen(key_path_tag); i++) {
        if (cmk_id_str[i] != key_path_tag[i]) {
            printf("ERROR(CLIENT): Invalid key path, it should be like \"%s1\".\n", key_path_tag);
            return false;
        }
    }

    for (size_t i = strlen(key_path_tag); i < strlen(cmk_id_str); i++) {
        if (cmk_id_str[i] < '0' || cmk_id_str[i] > '9') {
            has_invalid_char = true;
        }
        tmp_str[tmp_pos] = cmk_id_str[i];
        tmp_pos++;
    }

    if (has_invalid_char) {
        printf("ERROR(CLIENT): Invalid key path, '%s' is expected to be an integer.\n", tmp_str);
        return false;
    }

    tmp_str[tmp_pos] = '\0';
    *cmk_id = atoi(tmp_str);

    return true;
}

bool create_cmk(unsigned int cmk_id)
{
    unsigned int cmk_len = 0;

    if (!get_cmk_len(cmk_id, &cmk_len)) {
        return false;
    }

    if (cmk_len != DEFAULT_CMK_LEN) {
        printf("ERROR(GS_KTOOL): Default cmk len is %u, but the len of cmk read from gs_ktool is %u.\n",
            DEFAULT_CMK_LEN, cmk_len);
        return false;
    }

    return true;
}

bool read_cmk_plain(const unsigned int cmk_id, unsigned char *cmk_plain)
{
    unsigned int cmk_len = 0;

    if (cmk_plain == NULL) {
        return false;
    }

    /* init LRU cache list */
    if (cmk_cache_list == NULL) {
        cmk_cache_list = init_cmk_cache_list();
        if (cmk_cache_list == NULL) {
            return false;
        }
    }

    /* case a : try to get cmk plain from cache */
    if (!get_cmk_from_cache(cmk_cache_list, cmk_id, cmk_plain)) {
        /* case b : failed to get cmk plian from cache, try to get it from gs_ktool */
        if (!get_cmk_plain(cmk_id, cmk_plain, &cmk_len)) {
            return false;
        }

        /* check the length of cmk plain read from gs_ktool */
        if (cmk_len != DEFAULT_CMK_LEN) {
            printf("ERROR(CLIENT): Default cmk len is %u, but the len of cmk read from gs_ktool is %u.\n",
                DEFAULT_CMK_LEN, cmk_len);
            return false;
        }

        push_cmk_to_cache(cmk_cache_list, cmk_id, cmk_plain);
    }

    return true;
}

bool encrypt_cek_use_aes256(const unsigned char *cek_plain, size_t cek_plain_size, unsigned char *cmk_plain,
    unsigned char *cek_ciph, size_t &cek_ciph_len)
{
    /* use cmk_plian to generate derived_cmk_plain */
    AeadAesHamcEncKey aes_and_hmac_cek = AeadAesHamcEncKey(cmk_plain, (size_t)DEFAULT_CMK_LEN);
    AeadAesHamcEncKey *p_aes_and_hmac_cek = &aes_and_hmac_cek;
    EncryptionType enc_type = EncryptionType::DETERMINISTIC_TYPE;

    /* encrypt cek with cmk */
    cek_ciph_len =
        encrypt_data(cek_plain, cek_plain_size, *p_aes_and_hmac_cek, enc_type, cek_ciph, AEAD_AES_256_CBC_HMAC_SHA256);
    if (cek_ciph_len <= 0) {
        printf("ERROR(CLIENT): Fail to encrypt cek with cmk.\n");
        return false;
    }

    return true;
}

bool decrypt_cek_use_aes256(const unsigned char *cek_cipher, size_t cek_cipher_size, unsigned char *cmk_plain,
    unsigned char *cek_plain, size_t *cek_plain_len)
{
    AeadAesHamcEncKey derived_cmk_plain = AeadAesHamcEncKey(cmk_plain, (size_t)DEFAULT_CMK_LEN);
    AeadAesHamcEncKey *p_derived_cmk_plain = &derived_cmk_plain;

    *cek_plain_len =
        decrypt_data(cek_cipher, cek_cipher_size, *p_derived_cmk_plain, cek_plain, AEAD_AES_256_CBC_HMAC_SHA256);
    if (*cek_plain_len <= 0) {
        printf("ERROR(CLIENT): Fail to dencrypt cek with cmk.\n");
        return false;
    }

    return true;
}

void free_cmk_plain_cache()
{
    free_cmk_cache_list(cmk_cache_list);
}