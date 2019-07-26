class ArticleNotFound(Exception):
    def __init__(self, hostname):
        self.hostname = hostname
