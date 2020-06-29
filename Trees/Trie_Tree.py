class Trie(object):

    def __init__(self):
        """
        Initialize your data structure here.
        """
        self.dict_trie = {}

    def insert(self, word):
        """
        Inserts a word into the trie.
        :type word: str
        :rtype: None
        """
        node = self.dict_trie
        for i in word:
            if i in node:
                node = node[i]
            else:
                node[i] = {"val":0}
                node = node[i]
        node["val"] = 1 

    def search(self, word):
        """
        Returns if the word is in the trie.
        :type word: str
        :rtype: bool
        """
        node = self.dict_trie
        for i in word:
            if i in node:
                node = node[i]
            else:
                return False
        return True if node["val"] == 1 else False
        

    def startsWith(self, prefix):
        """
        Returns if there is any word in the trie that starts with the given prefix.
        :type prefix: str
        :rtype: bool
        """
        node = self.dict_trie
        for i in prefix:
            if i not in node:
                return False
            else:
                node = node[i]
        return True

if __name__ == '__main__':
    pre_tree = Trie()
    words = ["Trie","insert","search","search","startsWith","insert","search"]
    for i in words:
        pre_tree.insert(i)
    print(pre_tree.search("Trie"))
    print(pre_tree.search("search"))
    print(pre_tree.search("inser"))
    
