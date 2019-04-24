import csv
import requests
import json
from db_postgres import insert_video, create_session

environments = {
    'production': {
        'input_file_path': "/Users/lazybeam/Desktop/Firework/LicensedContent/videos/",
        'api_base_endpoint_url': "http://fireworktv.com",
        'operator_file_path': "/Users/lazybeam/Desktop/Firework/LicensedContent/accounts.csv",
        'video_upload_file_path': "/Users/lazybeam/Desktop/Firework/LicensedContent/videos.csv",
        'debug_mode': True
    },
    'staging': {
        'input_file_path': "/Users/lazybeam/Desktop/Firework/LicensedContent/videos/",
        'api_base_endpoint_url': "http://staging.fireworktv.com",
        'operator_file_path': "/Users/lazybeam/Desktop/Firework/LicensedContent/accounts.csv",
        'video_upload_file_path': "/Users/lazybeam/Desktop/Firework/LicensedContent/videos.csv",
        'debug_mode': True
    }
}

error_log = []
success_list = []

config = environments['production']


def update_video_upload_file(list_id, video_id):
    with open(config['video_upload_file_path'], 'r') as f1:
        video_list = csv.DictReader(f1)

        new_video_list = []
        headers = video_list.fieldnames

        for video in video_list:
            if video['no'] == str(list_id) and video['video_id'] is '':
                video['video_id'] = video_id
            new_video_list.append(video)

        f1.close()

    with open(config['video_upload_file_path'], 'w') as f2:
        writer = csv.DictWriter(f2, fieldnames=headers)
        writer.writeheader()
        writer.writerows(new_video_list)
        print('Successfully updated video upload file with list_id: %s -> video_id: %s' % (list_id, video_id))

        f2.close()


def main():
    f1 = open(config['video_upload_file_path'], 'r')
    metadata_list = csv.DictReader(f1)

    video_list = []

    for metadata in metadata_list:
        video_list.append(metadata)

    f1.close()

    for i, row in enumerate(video_list):
        print(i)
        print(row)
        try:
            # extract video metadata
            id = int(row['no'])
            original_file_name = row['file_name']
            raw_file_name = row['file_name']
            if raw_file_name:
                file_name = raw_file_name.replace("'", "_").replace('/', '_').replace("?", "_").replace(":", '_').replace('.mp4', '').replace('.mov', '').replace('&', '_').replace('.MOV', '')

                username = row['username']
                caption = row['title']

                hashtag = row['topic']
                hashtags = []
                if hashtag:
                    hashtags = hashtag.replace(' ', '').split(',') if hashtag is not None else None

                video_type = 'frameless' if row['afs_friendly'] == 'Y' else None

                naboo_video_id = row.get('video_id', None)

                if not naboo_video_id:

                    print('\n\nid: %s' % id)
                    print('username: %s' % username)
                    print('file_name: %s' % file_name)
                    print('topics: %s' % hashtags)
                    print('video_type: %s' % video_type)

                    # find corresponding operator info
                    account = {}
                    f2 = open(config['operator_file_path'], 'r')

                    accounts = csv.DictReader(f2)

                    for user in accounts:
                        if user['USERNAME'] == username:
                            account = user
                            print('Matched user info found: %s' % account)
                        else:
                            continue

                    if not account:
                        print('No matched username for user %s with id %s' % (username, id))

                    else:
                        jwt_token = account['JWT_TOKEN']

                        video_id = post(
                            video_path=config['input_file_path'] + file_name + '.mp4',
                            video_caption=caption,
                            jwt_token=jwt_token,
                            hashtags=hashtags,
                            original_file_name=original_file_name,
                            list_id=id,
                            video_type=video_type
                        )
                        # update video upload file
                        if video_id:
                            update_video_upload_file(id, video_id)

                        success_list.append(id)
                        # set the video to a state
            print('------------------------------------>\n')
        except Exception as error:
            print(error)
            error_log.append({'id': id, 'error_message': error, 'file_name': file_name, 'caption': caption,
                              'original_file_name': original_file_name})

    print('\n\n\n\nerror log:%s' % error_log)


def post(video_path, video_caption, jwt_token, hashtags, original_file_name, list_id, video_type):
    filename = video_path.split('/')[-1]
    files = {'file': ('file.mp4', open(video_path, 'rb'))}

    # header with jwt_token
    headers = {'Authorization': 'Bearer: %s' % jwt_token,
               'Content-Type': 'application/json'}

    # calling Naboo to get S3 credentials for file upload
    signature, status = __get_s3_credentials_from_server(
        headers=headers,
        filename=filename
    )

    if status is not False:
        # upload video file to S3 bucket and receive
        status = __put_video_in_s3_bucket(
            signature=signature,
            files=files
        )

        if status is not False:
            # with credential and payload, create a new video on the server
            status, response = __create_video_on_server(
                headers=headers,
                signature=signature,
                video_caption=video_caption,
                hashtags=hashtags,
                video_type=video_type
            )

            # getting video id from Naboo's response
            video_id = json.loads(response.text)['id']

            print("video id: %s" % video_id)

            # write to database upon successful upload
            session = create_session()
            db_status = insert_video(
                s=session,
                path=original_file_name,
                video_id=video_id,
                list_id=list_id,
                video_type=video_type
            )

            if db_status is not False:
                print("successfully recorded new video information in the database")
                return video_id
            else:
                print("failed to record new video information in the database")

            if status is not False:
                print("successfully created a new video on server")
            else:
                print("failed to cr eate new video on server")
        else:
            print("failed to upload video in S3 bucket")
    else:
        print("failed to get S3 credentials from the server")


def __get_s3_credentials_from_server(headers, filename):
    get_s3_credentials_endpoint = "%s/api/upload_signatures" % config["api_base_endpoint_url"]

    data = dict(
        filename=filename,
        mime_type='video/mp4'
    )

    response = requests.post(
        get_s3_credentials_endpoint,
        headers=headers,
        data=json.dumps(data)
    )

    if config["debug_mode"] is not False:
        print("response: %s" % response.text)
        print(response)

    signature = json.loads(response.text)

    if "201" in str(response):
        return signature, True
    else:
        return None, False


def __put_video_in_s3_bucket(signature, files):
    payload = {'key': signature["key"],
               'AWSAccessKeyId': signature["AWSAccessKeyId"],
               'acl': signature["acl"],
               'success_action_status': signature["success_action_status"],
               'policy': signature["policy"],
               'signature': signature["signature"],
               'Content-Type': signature["Content-Type"]}

    response = requests.post(signature["action"], data=payload, files=files)

    if "201" in str(response):
        return True
    else:
        return False


def __create_video_on_server(headers, signature, video_caption, hashtags, video_type):
    data = dict(
        key=signature["key"],
        caption=video_caption,
        hashtags=hashtags,
        labels=['manual_upload'],
        cover_time='00:00:1',
        video_type=video_type
    )

    create_video_endpoint = "%s/api/videos" % config["api_base_endpoint_url"]

    response = requests.post(create_video_endpoint, data=json.dumps(data), headers=headers)

    if "201" in str(response):
        return True, response
    else:
        return False, response


if __name__ == "__main__":
    main()