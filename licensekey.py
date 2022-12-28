
import json
from cryptography.fernet import Fernet
from datetime import datetime, timedelta

# Generate a key
key = Fernet.generate_key()

# Create a Fernet object using the key
fernet = Fernet(key)

# Define the data to be encrypted
data = {
    'Organization_name': 'ZingMind',
    'expiry_date':  datetime(2022, 12,30),
    'message': 'My secret message'
}
data['expiry_date'] = data['expiry_date'].strftime('%Y-%m-%d')

# Serialize the data using json
serialized_data = json.dumps(data).encode()

# Encrypt the serialized data
encrypted_data = fernet.encrypt(serialized_data)

# Print the encrypted data
#print(encrypted_data)

# Decrypt the encrypted data
decrypted_data = fernet.decrypt(encrypted_data)

# Deserialize the decrypted data using json
data = json.loads(decrypted_data.decode())

# Print the decrypted data
#print(data)