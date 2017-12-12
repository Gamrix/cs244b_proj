from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto import Random

# Example of how to use pycrypto package for digital signature
def pycrpyto_sig_demo():
    random_generator = Random.new().read
    key = RSA.generate(1024, random_generator)

    text = 'abcdefgh'
    hash = SHA256.new(text).digest()
    
    signature = key.sign(hash, '')
    print(signature)
    
    public_key = key.publickey()
    
    verification = public_key.verify(hash, signature)
    print(verification)
    