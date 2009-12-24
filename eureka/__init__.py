class EurekaException(Exception):
    def __init__(self, arg):
        return super(EurekaException, self) \
               .__init__(arg.encode('utf-8', 'xmlcharrefreplace'))
