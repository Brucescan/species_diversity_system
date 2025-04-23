var JSEncrypt = require('node-encrypt-js')
var crypto = require("crypto")
var CryptoJS = require("crypto-js")


function sort_ASCII(obj) {
    var arr = new Array;
    var num = 0;
    for (var i in obj) {
        arr[num] = i;
        num++
    }
    var sortArr = arr.sort();
    var sortObj = {};
    for (var i in sortArr) {
        sortObj[sortArr[i]] = obj[sortArr[i]]
    }
    return sortObj
}
function getUuid() {
    var s = [];
    var hexDigits = "0123456789abcdef";
    for (var i = 0; i < 32; i++) {
        s[i] = hexDigits.substr(Math.floor(Math.random() * 16), 1)
    }
    s[14] = "4";
    s[19] = hexDigits.substr(s[19] & 3 | 8, 1);
    s[8] = s[13] = s[18] = s[23];
    var uuid = s.join("");
    return uuid
}

function dataTojson(data) {
    var arr = [];
    var res = {};
    arr = data.split("&");
    for (var i = 0; i < arr.length; i++) {
        if (arr[i].indexOf("=") != -1) {
            var str = arr[i].split("=");
            if (str.length == 2) {
                res[str[0]] = str[1]
            } else {
                res[str[0]] = ""
            }
        } else {
            res[arr[i]] = ""
        }
    }
    return res
}


function my_md5(s){
    return crypto.createHash("md5").update(s).digest("hex")
}

//  加密请求头代码 请求加密(包括请求头以及参数加密)
function encryptHeaders(options){
    var paramPublicKey = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCvxXa98E1uWXnBzXkS2yHUfnBM6n3PCwLdfIox03T91joBvjtoDqiQ5x3tTOfpHs3LtiqMMEafls6b0YWtgB1dse1W5m+FpeusVkCOkQxB4SZDH6tuerIknnmB/Hsq5wgEkIvO5Pff9biig6AyoAkdWpSek/1/B7zYIepYY0lxKQIDAQAB";
    var encrypt = new JSEncrypt;
    encrypt.setPublicKey(paramPublicKey);
    var timestamp = Date.parse(new Date);
    var requestId = getUuid();
    var data = JSON.stringify(sort_ASCII(dataTojson(options.data || "{}")));
    options.data = encrypt.encryptLong(data);
    var sign = my_md5(data + requestId + timestamp);
    return {
        "sign":sign,
        'requestId':requestId,
        "timestamp":timestamp,
        "urlParam":options.data,
    }
}

function getMapping(_0x509dcf) {
    var _0x2b7e20 = {};
    _0x2b7e20['LbQfg'] = "53536868555767547048526949655455";
    var _0x245e3d = _0x2b7e20;
    for (var _0x1b74b3 = '', _0x33c0cd = 0x0; _0x33c0cd < _0x509dcf['length']; _0x33c0cd += 0x2) {
        var _0x37cad4 = _0x509dcf["substring"](_0x33c0cd, _0x33c0cd + 0x2);
        _0x1b74b3 += String['fromCharCode'](_0x37cad4);
    }
    return _0x1b74b3;

}
function decryptFn(data) {
    var _0x3c6fa1 = CryptoJS['enc']['Utf8']['parse'](getMapping('6756696653534952657053656868665752665050485566485667545454484967'))
      , _0x3ec027 = CryptoJS['enc']['Utf8']['parse'](getMapping('53536868555767547048526949655455'));
    return CryptoJS['AES']['decrypt'](data, _0x3c6fa1, {
        'iv': _0x3ec027,
        'mode': CryptoJS['mode']['CBC'],
        'padding': CryptoJS['pad']['Pkcs7']
    })['toString'](CryptoJS['enc']['Utf8']);
}
