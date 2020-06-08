import _set_path  # noqa
import i18n
import json


# python -c 'from handler import *; test()'
def test():
    i18n.set('file_format', 'json')
    i18n.load_path.append('./translation')

    i18n.set('locale', 'en')
    print(i18n.t('BPP1001_EAD.title'))
    print(i18n.t('BPP1001_EAD.description'))
    print(i18n.t('BPP1001_EAD.fallback'))
    print(i18n.t('BPP1001_EAD.nokey'))

    i18n.set('locale', 'es')
    print(i18n.t('BPP1001_EAD.title'))
    print(i18n.t('BPP1001_EAD.description'))
    print(i18n.t('BPP1001_EAD.fallback'))
    print(i18n.t('BPP1001_EAD.nokey'))

    print("use internal loader")
    with open("./i18n-test.json") as f:
        data = json.load(f)

    for lang, keys in data['i18n'].items():
        for key, value in keys.items():
            i18n.add_translation('BPP1001_EAD.' + key, value, lang)

    i18n.set('locale', 'en')
    print(i18n.t('BPP1001_EAD.title'))
    print(i18n.t('BPP1001_EAD.description'))
    print(i18n.t('BPP1001_EAD.fallback'))
    print(i18n.t('BPP1001_EAD.nokey'))

    i18n.set('locale', 'es')
    print(i18n.t('BPP1001_EAD.title'))
    print(i18n.t('BPP1001_EAD.description'))
    print(i18n.t('BPP1001_EAD.fallback'))
    print(i18n.t('BPP1001_EAD.nokey'))
