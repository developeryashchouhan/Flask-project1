import licensekey

LICENSE_KEY = licensekey.encrypted_data
DECRYPTION_DATA=licensekey.data
DECRYPTION_KEY=licensekey.decrypted_data
#print(LICENSE_KEY)


# class Config(object):
#     DEBUG = False
#     TESTING = False
#     SECRET_KEY= secrets.token_hex(16)
#     DB_NAME=""
#     DB_USERNAME=""
#     DB_PASSWORD=""
#     UPLOADS=""


#     SESSION_COOKIE_SECURE=True 

# class ProductionConfig(Config):
#     pass

# class DevelopmentConfig(Config):
#     DEBUG = False
    
#     SECRET_KEY= secrets.token_hex(16)
#     DB_NAME= "data_validation"
#     DB_USERNAME="root"
#     DB_PASSWORD="root"
    
#     SQLALCHEMY_DATABASE_URI='mysql://root:root@localhost/data_validation' 
#     SQLALCHEMY_TRACK_MODIFICATIONS = False
#     SESSION_PERMANENT = False
#     SESSION_TYPE = "filesystem"
    
  
    

#     SESSION_COOKIE_SECURE=True 

# class TestingConfig(Config):
    
#     TESTING = False
#     SECRET_KEY= ""
#     DB_NAME=""
#     DB_USERNAME=""
#     DB_PASSWORD=""
#     UPLOADS=""

#     SESSION_COOKIE_SECURE=True 