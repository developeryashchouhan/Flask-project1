from cryptography.hazmat.primitives import serialization, asymmetric, hashes
from cryptography.hazmat.backends import default_backend
from datetime import datetime
import time 
import json


def is_valid_license():
    private_key = asymmetric.rsa.generate_private_key(
        public_exponent=65537, 
        key_size=2048
    )

    public_key = private_key.public_key()
    data = {
        "organization": "Zing Mind",
        "expiry_date": "2022-12-31",
        "other_info": "additional_information"
    }
    json_data = json.dumps(data)

    ciphertext = public_key.encrypt(
        json_data.encode(),
        asymmetric.padding.OAEP(
            mgf=asymmetric.padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

    with open('encrypted_license.key', 'wb') as f:
        f.write(ciphertext)

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    with open('private.pem', 'wb') as f:
        f.write(private_pem)

    with open('encrypted_license.key', 'rb') as f:
        ciphertext = f.read()

    with open('private.pem', 'rb') as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
            backend=default_backend()
        )

    decrypted_string = private_key.decrypt(ciphertext,asymmetric.padding.OAEP(mgf=asymmetric.padding.MGF1(algorithm=hashes.SHA256()),algorithm=hashes.SHA256(),
        label=None
    )).decode()
    decrypted_data=json.loads(decrypted_string)
    key_date=decrypted_data["expiry_date"]
    key_datetime = datetime.strptime(key_date, '%Y-%m-%d')
    key_epoch = int(time.mktime(key_datetime.timetuple()))
    now = datetime.utcnow()
    now_epoch = int(time.mktime(now.timetuple()))
    diff = now_epoch - key_epoch


    if 'Zing Mind' == decrypted_data['organization'] and diff < 31536000:
        #print("Valid License Key")
        return True
    else:
        #print("Your License Key is expired ")
        return False

# def my_restricted_feature():
     
#     if is_valid_license():
#         print("Access granted. Welcome to the restricted feature.")
        
#     else:
#         print("Invalid License Key. Access denied.")

# my_restricted_feature()  
# 
   
