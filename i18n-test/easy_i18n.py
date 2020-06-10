

class EasyI18n():
    def __init__(self, data, defaultLocale="en"):
        self.defaultLocale = "en"
        self.data = data

    def t(self, key, locale=False):
        if not locale:
            locale = self.defaultLocale

        if self.data.get('i18n', {}).get(locale, {}).get(key, False):
            return self.data.get('i18n', {}).get(locale, {}).get(key, False)

        if locale != self.defaultLocale and self.data.get('i18n', {}).get(self.defaultLocale, {}).get(key, False):
            return self.data.get('i18n', {}).get(self.defaultLocale, {}).get(key, False)

        return key

    def setLocale(self, locale):
        self.defaultLocale = locale
