import os
import time
import pandas as pd
import json
from flask import Flask, request, jsonify, render_template, session,flash, redirect,url_for,send_file
import jwt
from datetime import date, datetime, timedelta
from functools import wraps
from configparser import ConfigParser
from flask_mysqldb import MySQL
import secrets
from werkzeug.utils import secure_filename 
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
from wtforms import StringField, PasswordField, BooleanField, StringField,  SubmitField
from wtforms.validators import InputRequired, Email, Length, DataRequired, Length,EqualTo
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user
from flask_migrate import Migrate
from flask_session import Session
from itsdangerous import URLSafeTimedSerializer as Serializer
import smtplib
import ssl
import datetime
import config


app = Flask(__name__)
app.config["DEBUG"] = True
app.config['SQLALCHEMY_DATABASE_URI']='mysql://root:12345@localhost/data_validation'
app.config['SECRET_KEY'] = secrets.token_hex(16)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
app.config.from_object(config)
license_key = app.config['LICENSE_KEY']
decryption_data=app.config['DECRYPTION_DATA']
decryption_key=app.config['DECRYPTION_KEY']

#app.config['SECRET_KEY']

Session(app)
Bootstrap(app) 
db = SQLAlchemy(app)
migrate = Migrate(app, db)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view= 'login'

# Intialize MySQL
mysql = MySQL(app)

basedir = os.path.abspath(os.path.dirname(__file__))

a=os.path.basename(os.path.dirname(__file__))
dirname=os.path.dirname(__file__)
UPLOAD_FOLDER = os.path.join(basedir,'/')
app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER

class User(UserMixin,db.Model):
    id = db.Column(db.Integer, primary_key =True)
    username = db.Column(db.String(100), nullable = False)
    email = db.Column(db.String(100), nullable= False)
    password = db.Column(db.String(150), nullable = False)
    role = db.Column(db.String(15))

    def __init__(self,username,email,password,role) :
        self.username=username
        self.email=email
        self.password=password
        self.role=role

    def __repr__(self):
            return '<User %r>' % self.username

    def get_token(self):
        serial=Serializer(app.config['SECRET_KEY'])
        return serial.dumps({'id':self.id}).encode().decode ('utf-8') 
    
    @staticmethod
    def verify_token(token):
        serial=Serializer(app.config['SECRET_KEY'])

        try:
            id=serial.loads(token)['id']
            print("id",id)
        except:
            return None
        return User.query.get(id)         
 
#with app.app_context():
#     db.create_all()

#     db.session.add(User('admin', 'admin@example.com','12345','user'))
#     db.session.add(User('guest', 'guest@example.com','12345','user'))
#     db.session.add(User('yash123', 'yash100chouhan@gmail.com','12345','admin'))
#     db.session.commit()

@login_manager.user_loader
def load_user(user_id):
	return User.query.get(user_id)

class ResetRequestForm(FlaskForm):
    email= StringField('email', validators=[InputRequired(), Length(min=5, max=45)])
    reset = SubmitField('reset')

class ResetPasswordForm(FlaskForm):
    password = PasswordField('password', validators=[InputRequired(),Length(min=5, max=10) ,EqualTo('confirm_password', message='Passwords must match')])
    confirm_password = PasswordField('confirm_password', validators=[InputRequired(), Length(min=5, max=10)])
    submit = SubmitField('submit') 

class LoginForm(FlaskForm):
	username= StringField('username', validators=[InputRequired(), Length(min=5, max=15)])
	password = PasswordField('password', validators=[InputRequired(), Length(min=5, max=10)])
	remember = BooleanField('Remember me')


class RegisterationForm(FlaskForm):
	email= StringField('Email', validators=[InputRequired(),Email(message='Invalid email'), Length(max=50)])
	username = StringField('username', validators=[InputRequired(), Length(min=4, max=15)])
	password = PasswordField('password', validators=[InputRequired(), Length(min=5, max=15)])

class UpdateForm(FlaskForm):
   
    email = StringField('email',Email(message=('Not a valid email address.')),[DataRequired()])
    username = StringField('username',[DataRequired()])
    submit = SubmitField('Submit')
     



def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        id=session.get("id")
        token=session.get("token")
        
        if not token:
            return jsonify({'message' : 'Unauthorized Access'}), 401
        if token:
            try:
                current_user = User.query.filter_by(id=id).first()
                
            except:
                return jsonify({'message': 'Something is missing in token'}), 401

            return f(current_user, *args, **kwargs)
    return decorated 


def send_mail(user):
    token=user.get_token()
    smtp_port = 587                 
    smtp_server = "smtp.gmail.com" 
    email_from = "yash100chouhan@gmail.com"
    email_to = user.email
    pswd = "qfgcupcdjotabklg"
    
    message = f'''To reset ur password click on link
           {url_for('reset_token',token=token,_external=True)}
            IF YOU DID'NT SEND A PASSWORD RESET REQUEST. PLEASE IGNORE THIS MESSAGE
   '''
    simple_email_context = ssl.create_default_context()
    try:
  
        print("Connecting to server...")
        TIE_server = smtplib.SMTP(smtp_server, smtp_port)
        TIE_server.starttls(context=simple_email_context)
        TIE_server.login(email_from, pswd)
        print("Connected to server :-)")
        print()
        print(f"Sending email to - {email_to}")
        TIE_server.sendmail(email_from, email_to, message)
        print(f"Email successfully sent to - {email_to}")
    except Exception as e:
        print(e)
    finally:
     TIE_server.quit()


@app.route('/reset_password',methods=['GET','POST'])
def reset_request():
    form=ResetRequestForm()
    if form.validate_on_submit():
        user=User.query.filter_by(email=form.email.data).first()
        if user:
            send_mail(user)
            print("message sent successfully!!!")
            flash('Reset request sent. Check your mail. ','success')
            return redirect(url_for('login'))
        else:
            flash('This email is not registered. please enter registered email','danger') 
            return redirect(url_for('reset_request'))       
    return render_template('reset_request.html',title='reset request',form=form)


@app.route('/reset_password/<token>',methods=['GET','POST'])
def reset_token(token):
    user= User.verify_token(token)
    if user is None:
        flash('That is invalid token or expired','warning')
        return redirect(url_for('reset_request'))
    form=ResetPasswordForm()
    if form.validate_on_submit():
        hashed_password = generate_password_hash(form.password.data, method= 'sha256')
        user.password=hashed_password
        db.session.commit()
        print("password changed")
        flash('password changed! please login!','success')
        return redirect(url_for('login'))
       

    return render_template('change_password.html',form=form)
       
@app.route('/', methods=['POST'])
def verify_key():
    # input_key = request.form['key']
    if license_key:
       now = datetime.datetime.now()
       date_format = "%Y-%m-%d"
       expiration_date = datetime.datetime.strptime(decryption_data["expiry_date"], date_format)
       

       if now > expiration_date:
            print("License has expired")
            flash('License has expired','danger')
            return render_template("index.html",key_valid=True)
       else:
            return redirect(url_for('login'))
    
        

@app.route('/',methods=['GET','POST'])
def index():
    verify_key()
    return render_template("index.html", key_valid=False)
    

@app.route('/login', methods=['GET', 'POST']) 
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data ).first()
        if not user:
            flash('Invalid User Name and Password','danger')       
            return render_template('login.html',form=form)
        if user:    
            if check_password_hash(user.password, form.password.data):
                    
                login_user(user, remember=form.remember.data)
                user1 = User.query.filter_by(username=form.username.data ).all()
                for data in user1:
                    if data.role =="admin":
                        session['logged_in']=True
                        token = jwt.encode({'id' : data.id,'exp' : datetime.datetime.utcnow() + timedelta(seconds=10)},app.config['SECRET_KEY'], "HS256")
                        session['id']=data.id
                        session['token']=token
                        return redirect(f"/admindashboard")
                    else:
                        
                        session['logged_in']=True
                        token = jwt.encode({'id' : data.id,'exp' : datetime.datetime.utcnow() + timedelta(seconds=10)},app.config['SECRET_KEY'], "HS256")
                        session['id']=data.id
                        session['token']=token
                        return redirect(f"/userdashboard")
            else:
                flash('Invalid User Name and Password','danger')      
                return render_template('login.html',form=form)
    user1 = User.query.filter_by(username=form.username.data ).all()
    return render_template('login.html',form=form,user=user1)

         
@app.route('/admindashboard',methods=['GET','POST'])
@token_required
@login_required
def admindashboard(current_user):
    if current_user:
        pass
        return render_template("admindashboard.html")


@app.route('/userdashboard',methods=['GET','POST'])
@token_required
@login_required
def userdashboard(current_user):
    if current_user:
        pass
        return render_template("userdashboard.html")    

# signup route
@app.route('/signup', methods =['POST','GET'])
@login_required
def signup():
    form = RegisterationForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username = form.username.data).first()
        if not user:
            user = User(
                username = form.username.data,
                email = form.email.data,
                password = generate_password_hash(form.password.data, method= 'sha256'),
                role='user'
                       )
            db.session.add(user)
            db.session.commit()
            flash('Successfully registered','success')
            return render_template('signup.html', form=form)
           
        else:
          
            flash('User already exists. Please Log in','warning')
            return render_template('signup.html', form=form)

    return render_template('signup.html', form=form)

@app.route('/manageusers',methods=['GET'])
@login_required
def manageusers():
    userDetails=User.query.all() 
    return render_template('manageusers.html',userDetails=userDetails)

@app.route('/update/<int:id>',methods=['GET'])
@login_required
def updateRoute(id):
    if not id or id != 0:
        Entry = User.query.get(id)
        if Entry:
            userDetails=User.query.filter_by(id=id).all()
            return render_template('update.html', userDetails=userDetails)

    
@app.route('/update/<int:id>', methods=['POST','PUT'])
@login_required
def update(id):
    
    if not id or id != 0:
        userDetails = User.query.get(id)
        if userDetails:
            new_email = request.form.get('email')
            new_username = request.form.get('username')
            userDetails.email = new_email
            userDetails.username = new_username
            db.session.commit()
        flash(' Updated Successfully','success')
        userDetails=User.query.all()
        return render_template('manageusers.html',userDetails=userDetails)

@app.route('/delete/<int:id>')
@login_required
def delete(id):
    if not id or id != 0:
        userDetails = User.query.get(id)
        if userDetails:
            db.session.delete(userDetails)
            db.session.commit()
        flash('Successfully Deleted','success')
        userDetails=User.query.all()
        return render_template('manageusers.html',userDetails=userDetails)

@app.route('/logout') 
def logout():
	logout_user()
	return redirect(url_for('index'))

#Data Validation source selection
@app.route('/Admin_data_validation',methods=['POST','GET'])
@login_required
def Admin_data_validation():
        if request.form["Submitbutton"]=='SingleDataSource':
            return render_template('AdminSingleDataSource.html')
        else:
            return render_template('AdminDoubleDataSource.html')
    
############ Admin Single Data Source Validation  ##########################

@app.route("/Admin_SingleDataSource", methods=['POST','GET'])
@login_required
def Admin_SingleDataSource():
    parser = ConfigParser()
    try:
        data_source_type = request.form['datasourcetype']
        if data_source_type=='CSV':             
            file = request.files['DataSourcePath']
            filename = secure_filename(file.filename)
            #file_path=os.path.join(basedir, file.filename)
            file_path = os.path.abspath('Store_File\\'+filename)
            delimiter = request.form['Delimiter']
            output_file_path = 'C:\\rulengine_master\Report'
            SKIP_ROWS = request.form['skip_rows']
            SHEET_NAME="None"
            Column_Address="None"
            Column_Address1="None"
            data = pd.read_csv(file_path,sep=delimiter,engine='python',encoding='latin1')          
            col_list = list(data.columns)
            # print(col_list)
            data_type_list = list(data.iloc[1])

        elif data_source_type=='XLSX' or data_source_type=='XLS':             
            file = request.files['DataSourcePath']
            filename = secure_filename(file.filename)
            #file_path=os.path.join(basedir, file.filename)
            file_path = os.path.abspath('Store_File\\'+filename)
            output_file_path = 'C:\\rulengine_master\Report'
            SKIP_ROWS = request.form['skip_rows']
            SHEET_NAME=request.form['sheet_name']
            Column_Address=request.form['Column_Address']
            Column_Address1=request.form['Column_Address1']
            data = pd.read_excel(file_path, engine='openpyxl',sheet_name=SHEET_NAME, skiprows=SKIP_ROWS, dtype=object)
            delimiter=','          
            col_list = list(data.columns)
         
         
            
        try:
             
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:     
                file.write("")  
            parser.add_section("APP")            
            parser.set("APP",'RULE_FILE_PATH',os.getcwd()+"\\rule_file.json")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE_PATH',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE",'SOURCE_DATA_FILE_PATH', file_path)
            parser.set("SOURCE",'SKIP_ROWS', SKIP_ROWS) 
            parser.set("SOURCE",'SHEET_NAME', SHEET_NAME)
            parser.set("SOURCE",'Column_Address', Column_Address)
            parser.set("SOURCE",'Column_Address1', Column_Address1)
           
            with open("C:\\rulengine_master\configuration.ini", 'w') as file: 
                parser.write(file)

        except:
             print(Exception)
             raise 
                                                                                            
        return render_template('rule_file_generator.html',file_path=file_path,delimiter=delimiter,data=data,file_name = filename, col_list=col_list,datatype_list=[get_datatype(data,colName) for  colName in col_list],len = len(col_list))
    except:
        print(Exception)
        raise
    

@app.route("/create", methods=['POST'])
@login_required 
def create_json():
    json_object = []
    try:
        i=1
        while True:
            Dict = {"RuleID": "" + str(i) + "",
            "RuleName": request.form[f"name{i}"] + " validation",            
            "DataAttribute": request.form[f'data_attribute{i}'],
            "DataType": request.form[f'datatype{i}'],
            "ValidationOperator": request.form[f'valop{i}'],
            "ValueToBeMatch": request.form[f'valtomatch{i}'],
            "Order": request.form[f'order{i}'],
            "DataObject":request.form['DataObject'],
            "DataSource":request.form['DataSource'],
            "Sequence":request.form[f'order{i}']
            
            }
            json_object = AddToJSON(json_object, Dict)
            i+=1
    except: 
        with open ('rule_file.json','w') as f:
            f.write(json.dumps(json_object,indent=4))    
        return render_template('download.html')


def AddToJSON(json_object, myDict):
    # Data to be written
    json_object.append(myDict)
    return json_object


def get_datatype(datafram,colName):
    try:
        if colName in datafram.columns:
            datatypes = datafram.dtypes[colName]
            if datatypes == 'object':
                return 'string'
            if datatypes == 'int64':
                return 'int' 
            if datatypes == 'float':
                return 'float'
            if datatypes == 'date':
                return 'date'
            if datatypes == 'time':
                return 'time'                       
            
    except:
        raise     
    # try:
    #     if type(col_name)==str:
    #         return 'string'
    #     if type(col_name.item())==int:
    #         return 'int'
    #     if type(col_name.item())==float:
    #         return 'float'
    #     if type(col_name.item())==time:
    #         return 'time'
    #     if type(col_name.item())==date:
    #         return 'date'
    # except:
    #     raise        


@app.route("/download")
@login_required
def download_file():
    downloaded_file="rule_file.json"
    return send_file(downloaded_file,as_attachment=True)

@app.route("/Regex")
@login_required
def Regex():
    return render_template('Regex.html')



# DOuble Data Source Validation



@app.route("/Admin_DoubleDataSource", methods=['POST','GET']) 
@login_required
def Admin_DoubleDataSource():
    
    parser = ConfigParser()
    try:
        with open("C:\\rulengine_master\configuration.ini", 'w') as file:
            file.write("")  
        output_file_path = request.form['output_file_path'] 
        
        

        data_source_type = request.form['datasourcetype']

        
        
        if data_source_type == 'CSV':

            file1 = request.files['DataSourcePath1'] 
            filename1=secure_filename(file1.filename)
            file_path1=os.path.join(basedir, file1.filename)
            delimiter1 = request.form['Delimiter1']
            
            parser.add_section("APP")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE","SOURCE_DATA_FILE_PATH", file_path1)
            parser.set("SOURCE","Delimiter", delimiter1)
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)
            
        if data_source_type == 'JSON':
            file_path1 = request.form['DataSourcePath1'] 
            parser.add_section("APP")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE","SOURCE_DATA_FILE_PATH", file_path1)
            
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)

        if data_source_type == 'XLSX':
            file_path1 = request.form['DataSourcePath1'] 
            sheet_no1 = request.form['sheet_no1'] 
            skip_rows1 = request.form['skip_rows1'] 

            parser.add_section("APP")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE","SOURCE_DATA_FILE_PATH", file_path1)
            parser.set("SOURCE","SHEET_NO", sheet_no1)
            parser.set("SOURCE","SKIP_ROWS", skip_rows1)
  
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)

        if data_source_type == 'ORACLE' or data_source_type == 'MYSQL':
            server1 = request.form['Server1'] 
            database1 = request.form['Database1'] 
            user1 = request.form['user1'] 
            password1 =file_path = request.form['password1'] 
            schema_name1 = request.form['schema_name1']            
            source_query_filter1 = request.form['source_query_filter1'] 
            parser.add_section("APP")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE","SERVER", server1)
            parser.set("SOURCE","DATABASE", database1)
            parser.set("SOURCE","USER", user1)
            parser.set("SOURCE","PASSWORD", password1)
            parser.set("vTurbineMasterData","SCHEMA_NAME", schema_name1)
            parser.set("vTurbineMasterData","SOURCE_QUERY_FILTER",source_query_filter1)

            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)
         
        # data dest
        

        data_source_type = request.form['datadesttype']
        parser.set("APP",'DEST_TYPE',data_source_type)

        if data_source_type == 'CSV':

            file2 = request.files['datasourcepath2'] 
            filename2=secure_filename(file2.filename)
            file_path2=os.path.join(basedir, file2.filename)
            delimiter1 = request.form['Delimiter1']
           
            delimiter2 = request.form['delimiter2']
           
            
            
            parser.add_section("DEST")
            parser.set("DEST","DEST_DATA_FILE_PATH", file_path2)
            parser.set("DEST","Delimiter", delimiter2)
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)
           
        if data_source_type == 'JSON':
            file_path2 = request.form['datasourcepath2'] 

            parser.add_section("DEST")
            parser.set("DEST","DEST_DATA_FILE_PATH", file_path2)
         
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)


        


        if data_source_type == 'XLSX':
            file_path2 = request.form['DataSourcePath2'] 
            sheet_no2 = request.form['sheet_no2'] 
            skip_rows2 = request.form['skip_rows2']

            parser.add_section("DEST")
            parser.set("DEST","DEST_DATA_FILE_PATH", file_path2)
            parser.set("DEST","SHEET_NO", sheet_no2)
            parser.set("DEST","SKIP_ROWS", skip_rows2)

            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)


        if data_source_type == 'ORACLE' or data_source_type == 'MYSQL':
            server2 = request.form['Server2'] 
            database2 = request.form['Database2'] 
            user2 = request.form['user2'] 
            password2 = request.form['password2'] 
            schema_name2 = request.form['schema_name2']            
            source_query_filter2 = request.form['source_query_filter2'] 
        
            parser.add_section("DEST")
            parser.set("DEST","SERVER", server2)
            parser.set("DEST","DATABASE", database2)
            parser.set("DEST","USER", user2)
            parser.set("DEST","PASSWORD", password2)

            parser.add_section("vTurbineMasterData")
            parser.set("vTurbineMasterData","SCHEMA_NAME", schema_name2)
            parser.set("vTurbineMasterData","SOURCE_QUERY_FILTER",source_query_filter2)

            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)
        
        
        return "success"
    except:
        print(Exception)
        raise
        
#######################   user   #################################

@app.route('/User_data_validation',methods=['POST','GET'])
@login_required
def User_data_validation():
        if request.form["Submitbutton"]=='SingleDataSource':
            return render_template('UserSingleDataSource.html')
        else:
            return render_template('UserDoubleDataSource.html')
    
#Single Data Source Validation

@app.route("/User_SingleDataSource", methods=['POST','GET'])
@login_required
def User_SingleDataSource():
    parser = ConfigParser()
    try:
        data_source_type = request.form['datasourcetype']
        if data_source_type=='CSV':             
            file = request.files['DataSourcePath']
            filename = secure_filename(file.filename)
            #file_path=os.path.join(basedir, file.filename)
            file_path = os.path.abspath('Store_File\\'+filename)
            delimiter = request.form['Delimiter']
            output_file_path = 'C:\\rulengine_master\Report'
            SKIP_ROWS = request.form['skip_rows']
            SHEET_NAME="None"
            Column_Address="None"
            Column_Address1="None"
            data = pd.read_csv(file_path,sep=delimiter,engine='python',encoding='latin1')          
            col_list = list(data.columns)
            # print(col_list)
            data_type_list = list(data.iloc[1])

        elif data_source_type=='XLSX' or data_source_type=='XLS':             
            file = request.files['DataSourcePath']
            filename = secure_filename(file.filename)
            #file_path=os.path.join(basedir, file.filename)
            file_path = os.path.abspath('Store_File\\'+filename)
            output_file_path = 'C:\\rulengine_master\Report'
            SKIP_ROWS = request.form['skip_rows']
            SHEET_NAME=request.form['sheet_name']
            Column_Address=request.form['Column_Address']
            Column_Address1=request.form['Column_Address1']
            data = pd.read_excel(file_path, engine='openpyxl',sheet_name=SHEET_NAME, skiprows=SKIP_ROWS, dtype=object)
            delimiter=','          
            col_list = list(data.columns)
         


        try:
             
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:     
                file.write("")  
            parser.add_section("APP")            
            parser.set("APP",'RULE_FILE_PATH',os.getcwd()+"\\rule_file.json")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE_PATH',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE",'SOURCE_DATA_FILE_PATH', file_path)
            parser.set("SOURCE",'SKIP_ROWS', SKIP_ROWS) 
            parser.set("SOURCE",'SHEET_NAME', SHEET_NAME)
            parser.set("SOURCE",'Column_Address', Column_Address)
            parser.set("SOURCE",'Column_Address1', Column_Address1)
           
            with open("C:\\rulengine_master\configuration.ini", 'w') as file: 
                parser.write(file)

        except:
             print(Exception)
             raise

        return render_template('rule_file_generator.html',file_path=file_path,delimiter=delimiter,data=data,file_name = filename, col_list=col_list,datatype_list=[get_datatype(data,colName) for  colName in col_list],len = len(col_list))
    except:
        print(Exception)
        raise
    




# DOuble Data Source Validation

@app.route("/User_DoubleDataSource", methods=['POST','GET']) 
@login_required
def User_DoubleDataSource():
    
    parser = ConfigParser()
    try:
        with open("C:\\rulengine_master\configuration.ini", 'w') as file:
            file.write("")  
        output_file_path = request.form['output_file_path']     
        

        data_source_type = request.form['datasourcetype']        
        
        if data_source_type == 'CSV':

            file1 = request.files['DataSourcePath1'] 
            filename1=secure_filename(file1.filename)
            file_path1=os.path.join(basedir+'Store_File\\'+file1.filename)
            print(file_path1)
            delimiter1 = request.form['Delimiter1']
            
            parser.add_section("APP")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE","SOURCE_DATA_FILE_PATH", file_path1)
            parser.set("SOURCE","Delimiter", delimiter1)
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)
            
        if data_source_type == 'JSON':
            file_path1 = request.form['DataSourcePath1'] 
            parser.add_section("APP")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE","SOURCE_DATA_FILE_PATH", file_path1)
            
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)

        if data_source_type == 'XLSX':
            file_path1 = request.form['DataSourcePath1'] 
            sheet_no1 = request.form['sheet_no1'] 
            skip_rows1 = request.form['skip_rows1'] 

            parser.add_section("APP")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE","SOURCE_DATA_FILE_PATH", file_path1)
            parser.set("SOURCE","SHEET_NO", sheet_no1)
            parser.set("SOURCE","SKIP_ROWS", skip_rows1)
  
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)

        if data_source_type == 'ORACLE' or data_source_type == 'MYSQL':
            server1 = request.form['Server1'] 
            database1 = request.form['Database1'] 
            user1 = request.form['user1'] 
            password1 =file_path = request.form['password1'] 
            schema_name1 = request.form['schema_name1']            
            source_query_filter1 = request.form['source_query_filter1'] 
            parser.add_section("APP")
            parser.set("APP",'SOURCE_TYPE',data_source_type)
            parser.set("APP",'OUTPUT_FILE',output_file_path)
            parser.add_section("SOURCE")
            parser.set("SOURCE","SERVER", server1)
            parser.set("SOURCE","DATABASE", database1)
            parser.set("SOURCE","USER", user1)
            parser.set("SOURCE","PASSWORD", password1)
            parser.set("vTurbineMasterData","SCHEMA_NAME", schema_name1)
            parser.set("vTurbineMasterData","SOURCE_QUERY_FILTER",source_query_filter1)

            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)
         
        # data dest 
        data_source_type = request.form['datadesttype']
        parser.set("APP",'DEST_TYPE',data_source_type)

        if data_source_type == 'CSV':

            file2 = request.files['datasourcepath2'] 
            filename2=secure_filename(file2.filename)
            file_path2=os.path.join(basedir, file2.filename)
            delimiter1 = request.form['Delimiter1']
           
            delimiter2 = request.form['delimiter2']
           
            
            
            parser.add_section("DEST")
            parser.set("DEST","DEST_DATA_FILE_PATH", file_path2)
            parser.set("DEST","Delimiter", delimiter2)
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)
           
        if data_source_type == 'JSON':
            file_path2 = request.form['datasourcepath2'] 

            parser.add_section("DEST")
            parser.set("DEST","DEST_DATA_FILE_PATH", file_path2)
         
            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)


        


        if data_source_type == 'XLSX':
            file_path2 = request.form['DataSourcePath2'] 
            sheet_no2 = request.form['sheet_no2'] 
            skip_rows2 = request.form['skip_rows2']

            parser.add_section("DEST")
            parser.set("DEST","DEST_DATA_FILE_PATH", file_path2)
            parser.set("DEST","SHEET_NO", sheet_no2)
            parser.set("DEST","SKIP_ROWS", skip_rows2)

            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)


        if data_source_type == 'ORACLE' or data_source_type == 'MYSQL':
            server2 = request.form['Server2'] 
            database2 = request.form['Database2'] 
            user2 = request.form['user2'] 
            password2 = request.form['password2'] 
            schema_name2 = request.form['schema_name2']            
            source_query_filter2 = request.form['source_query_filter2'] 
        
            parser.add_section("DEST")
            parser.set("DEST","SERVER", server2)
            parser.set("DEST","DATABASE", database2)
            parser.set("DEST","USER", user2)
            parser.set("DEST","PASSWORD", password2)

            parser.add_section("vTurbineMasterData")
            parser.set("vTurbineMasterData","SCHEMA_NAME", schema_name2)
            parser.set("vTurbineMasterData","SOURCE_QUERY_FILTER",source_query_filter2)

            with open("C:\\rulengine_master\configuration.ini", 'w') as file:
                parser.write(file)
        
        
        return "success"
    except:
        print(Exception)
        raise



#app run
if (__name__ == "__main__"):
     app.run(debug=True)

