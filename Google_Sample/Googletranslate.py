import execjs
import json
import os
import requests
from httpcore import SyncHTTPProxy
from googletrans import Translator

def get_apikey(path):
    if path == None:
        path = './GoogleTranslate_APIKEY.json'
    if not os.path.exists(path):
        raise FileNotFoundError("{} is not exists".format(path))
    with open(path, 'rb+') as f:
        apikey_dic = json.load(f)
        f.close()
    return apikey_dic['API_KEY']
def GoogleTranslate_apikey(target, API_KEY, text):
    """Translates text into the target language.

    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    """
    url = "https://translation.googleapis.com/language/translate/v2?target={}&key={}&q={}".format(target, API_KEY, text)
    translate_result = "Translate failed by google"
    try:
        http_proxy = SyncHTTPProxy((b'http', b'127.0.0.1', 8001, b''))
        proxies = {'HTTP': http_proxy, 'HTTPS': http_proxy}
        headers = {'cookie':'BIDUPSID=07CE77D3C49F1E65504D32486E7ADABD; PSTM=1550765123; __yjs_duid=1_28449a23a25f1f66849f07643cdba8f31620222174447; BDUSS=BVbjJPN3dtTDdIR1hneGp5RjU5RE9KekNqOGFBRU9hajJCdTZHfjNqLU5hQjVoSVFBQUFBJCQAAAAAAAAAAAEAAACuML9muaXG77-hMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAI3b9mCN2~Zgbn; BDUSS_BFESS=BVbjJPN3dtTDdIR1hneGp5RjU5RE9KekNqOGFBRU9hajJCdTZHfjNqLU5hQjVoSVFBQUFBJCQAAAAAAAAAAAEAAACuML9muaXG77-hMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAI3b9mCN2~Zgbn; REALTIME_TRANS_SWITCH=1; FANYI_WORD_SWITCH=1; HISTORY_SWITCH=1; SOUND_SPD_SWITCH=1; SOUND_PREFER_SWITCH=1; H_WISE_SIDS=110085_127969_131861_174441_176399_179346_184716_188746_189755_190625_194085_194529_196528_197241_197711_197957_198257_199022_199582_201193_202652_203309_203517_204122_204427_204713_204715_204717_204720_204864_204947_205218_205413_205423_205485_206007_206704_207236_207264_207830_207888_208114_208268_208344_208493_208523_208721_208756_208806_208968_209063_209394_209512_209568_209576_209842_209944_209981_210127_210164_210307_210359_210519_210583_210665_210670_210710_210733_210736_210837_210891_210892_210894_210900_210907_211023_211062_211116_211180_211242_211302_211311_211331_211351_211417_211442_211457_211580_211587_211783_212485_212618_212701_212770; APPGUIDE_10_0_2=1; BAIDUID=4DB8D36707E419312496B710F9B5606C:FG=1; H_WISE_SIDS_BFESS=110085_127969_131861_174441_176399_179346_184716_188746_189755_190625_194085_194529_196528_197241_197711_197957_198257_199022_199582_201193_202652_203309_203517_204122_204427_204713_204715_204717_204720_204864_204947_205218_205413_205423_205485_206007_206704_207236_207264_207830_207888_208114_208268_208344_208493_208523_208721_208756_208806_208968_209063_209394_209512_209568_209576_209842_209944_209981_210127_210164_210307_210359_210519_210583_210665_210670_210710_210733_210736_210837_210891_210892_210894_210900_210907_211023_211062_211116_211180_211242_211302_211311_211331_211351_211417_211442_211457_211580_211587_211783_212485_212618_212701_212770; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; Hm_lvt_64ecd82404c51e03dc91cb9e8c025574=1672465734,1672572822,1672805608; BAIDUID_BFESS=4DB8D36707E419312496B710F9B5606C:FG=1; RT="z=1&dm=baidu.com&si=85vr94sptu7&ss=lch5d7yv&sl=3&tt=1wd&bcn=https://fclog.baidu.com/log/weirwood?type=perf&ld=3f7&ul=130e&hd=131f"; PSINO=7; delPer=0; BA_HECTOR=8580012g2k810h0024ag06s01hra0g41i; ZFY=s:AXDUM7VVX8gihQULO2OeqNKb3cz2Gah7KoNJHiUKu4:C; BDRCVFR[feWj1Vr5u3D]=I67x6TjHwwYf0; BDRCVFR[dG2JNJb_ajR]=mk3SLVN4HKm; BDRCVFR[-pGxjrCMryR]=mk3SLVN4HKm; H_PS_PSSID=36557_37973_37647_37554_38023_37906_38019_36920_38035_37990_37937_37904_26350_37881; ZD_ENTRY=google; Hm_lpvt_64ecd82404c51e03dc91cb9e8c025574=1672829411; ab_sr=1.0.1_MDAwZWEyMTQ2ZWJlMjk2ZTYyMjI3YWE2MWY2Y2ZlNmFkOTQwM2ZlYjY1NzQ3M2I3YzE1NGE2MDZiOTY3OGUyNDQ3NWY3NjlmNDUwOGUzOTQzMjQwMDFlODhmM2E3MWY3MTJhMDQ2YWQ4ZTAyYjk4YTEyYTQxZWY3Y2U3NGY0YzUxZGYxZWZmOTFiMDVkYjg3MDM1Yjg1NGM0NzdiYzRmNTQ1NTVjOTY2NzhlYjAzODQyNmIwMzg0MWU1ZWZjYzI0'}
        r = requests.get(url,headers=headers,proxies = proxies)
        trans_result = r.json()['data']['translations'][0]['translatedText']
    except Exception as e:
        print(str(e))
    return translate_result
def GoogelTranslate_IAM(target, text):
    """Translates text into the target language.

    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    Please generate a json of IAM firstly, then provide authentication credentials
     to your application code or commands. To do this, set the environment variable
     to GOOGLE_APPLICATION_CREDENTIALSthe path of the JSON file that contains your service account key.

    such as "export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/service-account.json"
    """
    import six
    from google.cloud import translate_v2 as translate
    os.environ['https_proxy'] = 'http://127.0.0.1:8001'

    translate_client = translate.Client()
    if isinstance(text, six.binary_type):
        text = text.decode("utf-8")

    # Text can also be a sequence of strings, in which case this method
    # will return a sequence of results for each text.
    result_translate = " "
    i = 0
    while(i<5):
        try:
            result = translate_client.translate(text, target_language=target)
            result_translate = result["translatedText"]
            if result_translate != None:
                i = 5
        except Exception as e:
            print(str(e))
            i+=1
    return result_translate
    #print(u"Text: {}".format(result["input"]))
    #print(u"Translation: {}".format(result["translatedText"]))
    #print(u"Detected source language: {}".format(result["detectedSourceLanguage"]))

def googletranslate(w):
    '''谷歌翻译'''
    http_proxy = SyncHTTPProxy((b'http', b'127.0.0.1', 8001, b''))
    trans = Translator(proxies = {'http':http_proxy, 'https':http_proxy})
    result_translate = " "
    try:
        t = trans.translate(w, dest = 'zh-cn')
        result_translate = t.text
    except Exception as e:
        print(str(e))

    return result_translate
    

#API_KEY = get_apikey(None)    
#translate_result = GoogleTranslate_apikey('en', API_KEY, '你好')
#target = 'en'
#text = '你好'
#GoogelTranslate_IAM(target, text)
#print(translate_result)
