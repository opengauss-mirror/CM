/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * CM is licensed under Mulan PSL v2.
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
 * cm_cipher.cpp
 *    cm communication with ssl
 *
 * IDENTIFICATION
 *    src/cm_communication/cm_protocol/cm_cipher.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cm_cipher.h"
#include "securec.h"
#include "openssl/rand.h"
#include "openssl/evp.h"
#include "openssl/ossl_typ.h"
#include "openssl/x509.h"
#include "openssl/ssl.h"
#include "openssl/asn1.h"
#include "openssl/hmac.h"
#include "cm/cm_elog.h"

/* get_evp_cipher_by_id: if you need to be use,you can add some types */
static const EVP_CIPHER *get_evp_cipher_by_id(uint32 alg_id)
{
    const EVP_CIPHER *evpCipher = NULL;
    switch (alg_id & 0xFFFF) {
        case NID_aes_128_cbc:
            evpCipher = EVP_aes_128_cbc();
            break;
        case NID_aes_256_cbc:
            evpCipher = EVP_aes_256_cbc();
            break;
        case NID_undef:
            evpCipher = EVP_enc_null();
            break;
        default:
            write_runlog(DEBUG1, "invalid algorithm for evpCipher");
            break;
    }
    return evpCipher;
}

/*
 * @Brief        : GS_UINT32 CRYPT_decrypt()
 * @Description  : decrypts cipher text to plain text using decryption algorithm.
 *		  It creates symmetric context by creating algorithm object, padding object,
 *		  opmode object. After decryption, symmetric context needs to be freed.
 * @return       : success: 0, failed: 1.
 *
 * @Notes        : the last block is not full. so here need to padding the last block.(the block size is an
 * algorithm-related parameter) 1.here *ISO/IEC 7816-4* padding method is adoptted:the first byte uses "0x80" to padding
 * ,and the others uses "0x00". Example(in the following example the block size is 8 bytes): when the last block is not
 * full: The last block has 4 bits,so padding is required for 4 bytes
 *                                ... | DD DD DD DD DD DD DD DD | DD DD DD DD 80 00 00 00 |
 *                       when the last block is full: here need to add a new block
 *                                ... | DD DD DD DD DD DD DD DD | 80 00 00 00 00 00 00 00 |
 */
static status_t CRYPT_decrypt(uint32 alg_id, const unsigned char *key,
    uint32 key_len, cipher_t *cipher, unsigned char *plain_text, uint32 *plain_len)
{
    errno_t rc;
    uint32 plain_size = *plain_len;
    const EVP_CIPHER *cipher_alg = get_evp_cipher_by_id(alg_id);
    if (cipher_alg == NULL) {
        return CM_ERROR;
    }

    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        return CM_ERROR;
    }
    (void)EVP_CipherInit_ex(ctx, cipher_alg, NULL, key, cipher->IV, CM_FALSE);

    (void)EVP_CIPHER_CTX_set_padding(ctx, CM_FALSE);

    uint32 dec_num = 0;
    if (!EVP_DecryptUpdate(ctx, plain_text, (int32*)&dec_num, cipher->cipher_text, cipher->cipher_len)) {
        EVP_CIPHER_CTX_free(ctx);
        return CM_ERROR;
    }

    *plain_len = dec_num;
    if (!EVP_DecryptFinal(ctx, plain_text + dec_num, (int32*)&dec_num)) {
        EVP_CIPHER_CTX_free(ctx);
        rc = memset_s(plain_text, plain_size, 0, plain_size);
        securec_check_errno(rc, (void)rc);
        return CM_ERROR;
    }

    *plain_len += dec_num;
    /* padding bytes of the last block need to be removed */
    uint32 block_size = (uint32)EVP_CIPHER_CTX_block_size(ctx);
    uint32 pwd_len = (*plain_len) - 1;
    while (*(plain_text + pwd_len) == 0) {
        pwd_len--;
    }

    if (pwd_len < ((*plain_len) - block_size) || *(plain_text + pwd_len) != 0x80) {
        EVP_CIPHER_CTX_free(ctx);
        rc = memset_s(plain_text, plain_size, 0, plain_size);
        securec_check_errno(rc, (void)rc);
        return CM_ERROR;
    }
    (*plain_len) = pwd_len;
    plain_text[pwd_len] = '\0';
    EVP_CIPHER_CTX_free(ctx);
    return CM_SUCCESS;
}

status_t cm_decrypt_pwd(cipher_t *cipher, unsigned char *plain_text, uint32 *plain_len)
{
    unsigned char key[RANDOM_LEN] = { 0 };

    /* get the decrypt key value */
    int32 ret = PKCS5_PBKDF2_HMAC((const char*)cipher->rand, RANDOM_LEN,
        cipher->salt, RANDOM_LEN, ITERATE_TIMES, EVP_sha256(), RANDOM_LEN, key);
    if (ret != 1) {
        write_runlog(DEBUG1, "PKCS5_PBKDF2_HMAC generate the derived key failed, errcode:%d", ret);
        return CM_ERROR;
    }

    /* decrypt the cipher */
    if (CRYPT_decrypt(NID_aes_128_cbc, key, RANDOM_LEN, cipher, plain_text, plain_len) != CM_SUCCESS) {
        return CM_ERROR;
    }
    (void)memset_s(key, RANDOM_LEN, 0, RANDOM_LEN);
    return CM_SUCCESS;
}
