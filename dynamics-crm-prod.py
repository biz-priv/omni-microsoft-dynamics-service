import mailparser
import json
import boto3
import requests

sns = boto3.client('sns')
client = boto3.client('s3')

def lambda_handler(event, context):
    print (json.dumps(event))
    key = event["Records"][0]["ses"]["mail"]["messageId"]
    print key
    fromaddress = event["Records"][0]["ses"]["mail"]["commonHeaders"]["from"][0]
    subject = event["Records"][0]["ses"]["mail"]["commonHeaders"]["subject"]
    response = get_mail_data('marketing28122', key, '/tmp/my_file')
    print (json.dumps(response))

    if 'notifications@unbounce.com' in fromaddress:
        load = unbounce(response, subject, key)
        dynamics_insert = dynamics365insert(load, key)
        return dynamics_insert
    elif 'marketing@omnilogistics.com' in fromaddress:
        load = marketing(response, subject, key)
        dynamics_insert = dynamics365insert(load, key)
        return dynamics_insert
    elif 'noreply@formstack.com' in fromaddress:
        load = yext(response, subject, key)
        dynamics_insert = dynamics365insert(load, key)
        return dynamics_insert
    else:
        msg = "S3 Bucket Key ID: "+key+".\nReceived Lead information: "+json.dumps(response)
        sub = "Not Defined from email address in: "+subject
        sns_notify(sub, msg)
        return msg
            
def strip_unwanted_data(body):
    return body.split("--- mail_boundary ---")[0]

def remove_empty_values(arr): 
    newArr = []
    for x in arr:
        if x!="":
            if x!="\r":
                newArr.append(x)
    return newArr

def make_json(arr):
    print (json.dumps(arr))
    result = {}
    remaining_arr = []
    for a in arr:
        if(': ' in a):
            subArray = a.split(': ')
            result[subArray[0]] = subArray[1].replace('\r','')
        else:
            remaining_arr.append(a)
    result['Other_info'] = remaining_arr
    return result

def get_mail_data(bucketName, key, fileName):
    client.download_file(bucketName, key, fileName) 
    mail = mailparser.parse_from_file(fileName)
    body = strip_unwanted_data(mail.body)
    body_obj = make_json(remove_empty_values(body.split('\n')))
    return body_obj

def unbounce(data, subject, key):
    d = data["device"] if "device" in data else ""
    if d == 'm':
        device = 'mobile'
    elif d == 't':
        device = 'tablet'
    elif d == 'c':
        device = 'computer'
    else:
        device = ""

    n = data["network"] if "network" in data else ""
    if n == 'g':
        network = 'Google search'
    elif n == 's':
        network = 'search partner'
    elif n == 'd':
        network = 'Display Network'
    else:
        network = ""

# To get country and state data specific to location_id
    loc_physical_ms = data["loc_physical_ms"] if "loc_physical_ms" in data else ""
    print loc_physical_ms
    loc_id = int(loc_physical_ms)
    location = unbounce_location(loc_id)
    country = location["country"]
    state = location["state"]

    data_return = {
    "topic": "Google Ad Lead - "+data["company_name"] if "company_name" in data else "Google Ad Lead - --",
    "firstname": data["first_name"] if "first_name" in data else "",
    "lastname": data["last_name"] if "last_name" in data else "--",
    "emailaddress": data["work_email"] if "work_email" in data else "",
    "company": data["company_name"] if "company_name" in data else "--",
    "state": state,
    "phone": data["phone_number"] if "phone_number" in data else data["work_phone_number"] if "work_phone_number" in data else "",
    "adgroupid": data["adgroupid"] if "adgroupid" in data else "",
    "device": device,
    "network": network,
    "country": country,
    "keyword": data["keyword"] if "keyword" in data else "",
    "loc_physical_ms": country,
    "lead_source": "Google Ads"
    }
    payload = json.dumps(data_return)
    print payload
    if data_return["lastname"] == "--":
        msg = "S3 Bucket Key ID: "+key+".\nLast name is missing for the Lead: "+payload
        sub = "Missing information in: "+subject
        sns_notify(sub, msg)
    elif data_return["company"] == "--":
        msg = "S3 Bucket Key ID: "+key+".\nCompany name is missing for the Lead: "+payload
        sub = "Missing information in: "+subject
        sns_notify(sub, msg)
    return payload

def yext(data, subject, key):
# To get country and state data specific to location_id
    page_source = data["Page Source"] if "Page Source" in data else ""
    print page_source
    source_info = yext_page_source(page_source)
    country = source_info["country"]
    state = source_info["state"]

    data_return = {
    "topic": "Yext Lead - "+data["Company"] if "Company" in data else "Yext Lead - --",
    "firstname": data["First Name"] if "First Name" in data else "",
    "lastname": data["Last Name"] if "Last Name" in data else "--",
    "emailaddress": data["Email"] if "Email" in data else "",
    "company": data["Company"] if "Company" in data else "--",
    "state": state,
    "phone": data["Phone"] if "Phone" in data else "",
    "country": country,
    "lead_source": "Location SEO"
    }
    payload = json.dumps(data_return)
    print payload
    if data_return["lastname"] == "--":
        msg = "S3 Bucket Key ID: "+key+".\nLast name is missing for the Lead: "+payload
        sub = "Missing information in: "+subject
        sns_notify(sub, msg)
    elif data_return["company"] == "--":
        msg = "S3 Bucket Key ID: "+key+".\nCompany name is missing for the Lead: "+payload
        sub = "Missing information in: "+subject
        sns_notify(sub, msg)
    return payload

def marketing(data, subject, key):
    source = data["State/Province"] if "State/Province" in data else data["Country"] if "Country" in data else ""
    if source:
        source_info = web_page_source(source)
        state2 = source_info["state"]

    data_return = {
    "topic": "Web Lead - "+data["Company"] if "Company" in data else "Web Lead - "+data["Organization Name"] if "Organization Name" in data else "Web Lead - --",
    "firstname": data["First Name"] if "First Name" in data else data["Name"].split(" ")[0] if "Name" in data else "",
    "lastname": data["Last Name"] if "Last Name" in data else data["Name"].split(" ")[1] if ("Name" in data) and (" " in data["Name"]) else "--",
    "emailaddress": data["Email"] if "Email" in data else "",
    "company": data["Company"] if "Company" in data else data["Organization Name"] if "Organization Name" in data else "--",
    "state": state2 if ("State/Province" in data) or ("Country" in data) else "--",
    "phone": data["Phone"] if "Phone" in data else "",
    "street": data["Street Address"] if "Street Address" in data else "",
    "city": data["City"] if "City" in data else "",
    "zip": data["Zip"] if "Zip" in data else "",
    "country": data["Country"] if "Country" in data else "",
    "lead_source": "Web Footer" if "Company" in data else "Web" if "Organization Name" in data else "--"
    }
    payload = json.dumps(data_return)
    print payload
    if data_return["lastname"] == "--":
        msg = "S3 Bucket Key ID: "+key+".\nLast name is missing for the Lead: "+payload
        sub = "Missing information in: "+subject
        sns_notify(sub, msg)
    elif data_return["company"] == "--":
        msg = "S3 Bucket Key ID: "+key+".\nCompany name is missing for the Lead: "+payload
        sub = "Missing information in: "+subject
        sns_notify(sub, msg)
    return payload

def unbounce_location(location_id):
    url = "https://script.google.com/macros/s/AKfycbxLo5Lp-gwoWNErfzGmoksy629aB5Wrkr44KksyEL1D26tkKNca/exec"
    payload = {}
    headers= {}
    response = requests.request("GET", url, headers=headers, data = payload)
    received_data = response.text.encode('utf8')
    y = json.loads(received_data)
    extracted_location = []
    for x in y["data"]:
        if x["id"] == location_id:
            t = {"state": x["state"],
            "country": x["country"]
            }
            extracted_location.append(t)

    h = {"state": "--",
    "country": ""
    }
    if len(extracted_location) == 0:
        return h
    else:
        return extracted_location[0]
        
def yext_page_source(ps):
    url = "https://script.google.com/macros/s/AKfycbxOsFb7_whOd4DKREG04n0vhX2T7KgTg9too_kyvbCQi7_q9XG5/exec"
    payload = {}
    headers= {}
    response = requests.request("GET", url, headers=headers, data = payload)
    received_data = response.text.encode('utf8')
    y = json.loads(received_data)
    extracted_source = []
    for x in y["data"]:
        if x["source"] == ps:
            t = {"state": x["state"],
            "country": x["country"]
            }
            extracted_source.append(t)

    not_found = {"state": "--",
    "country": ""
    }
    if len(extracted_source) == 0:
        return not_found
    else:
        return extracted_source[0]
        
def web_page_source(ps):
    url = "https://script.google.com/macros/s/AKfycbzbgDuM9tvOXy0YZ3QVD2Ow3HlN-uLdqnLBzrefdGxwwZwLsZxZ/exec"
    payload = {}
    headers= {}
    response = requests.request("GET", url, headers=headers, data = payload)
    received_data = response.text.encode('utf8')
    y = json.loads(received_data)
    extracted_source = []
    for x in y["data"]:
        if (x["country"] == ps) or (x["state"] == ps):
            t = {"state": x["result"]
            }
            extracted_source.append(t)

    not_found = {"state": ps
    }
    if len(extracted_source) == 0:
        return not_found
    else:
        return extracted_source[0]


def dynamics365insert(payload, key):
    print ("3rd")
    url = "https://prod-44.westus.logic.azure.com:443/workflows/eccd8f1502ec4a01b5f9c0d654b051eb/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=RymgIUikvceerfB8YYbPeum87OOy5Alb4QS664Sfdo0"
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data = payload)
    request = response.text.encode('utf8')

    if not request:
        return "Data inserted into Dynamics CRM"
    else:
        subject = "Lead was not inserted into Dynamics365 CRM"
        message = "S3 Bucket Key ID: "+key+".\nLead Information: "+payload
        sns_notify(subject, message)

    print request
    return request

def sns_notify(subject, msg):
    sub = subject
    response = sns.publish(
    TopicArn='arn:aws:sns:us-east-1:332281781429:dynamics_crm_notify',
    Subject= sub,
    Message= msg,   
    )
    # Print out the response
    print response
    return response