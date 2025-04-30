from datetime import datetime, timezone, timedelta
from typing import List, Dict

def process_zoom_participants(all_participants: List[dict]) -> List[Dict[str, str]]:
    """
    Preprocess Zoom participant data with the following steps:
    1. Remove internal users (except Carl Helgesson)
    2. Remove duplicates based on registrant_id
    3. Simplify participant data to name, email, webinar_date, and duration
       - Only the first entry with the name "Carl Helgesson" retains the 'webinar_end_time' key.
    4. Format webinar_date to 'YYYY-MM-DD' and webinar_end_time to 'YYYY-MM-DD h:mm AM/PM'.
    5. Adjust all times by adding one hour to reflect the correct webinar time.

    Args:
        all_participants (List[dict]): Raw Zoom participant data

    Returns:
        List[Dict[str, str]]: Processed participant data
    """
    # First, filter out internal users (keeping Carl Helgesson)
    filtered_participants = [
        participant for participant in all_participants 
        if not participant['internal_user'] or participant['name'] == 'Carl Helgesson'
    ]

    # Remove duplicates by keeping the first occurrence of each registrant_id
    unique_participants = {}
    for participant in filtered_participants:
        if participant['registrant_id'] not in unique_participants:
            unique_participants[participant['registrant_id']] = participant

    # Track if "Carl Helgesson" has already been processed
    carl_processed = False

    # Simplify the data to just name, email, webinar_date, and duration
    processed_participants = []
    for participant in unique_participants.values():
        # Adjust times by adding one hour
        adjusted_join_time = participant['join_time'] + timedelta(hours=1)
        adjusted_leave_time = participant['leave_time'] + timedelta(hours=1)

        processed_participant = {
            'name': participant['name'],
            'user_email': participant['user_email'],
            'webinar_date': adjusted_join_time.strftime('%Y-%m-%d'),  # Format to only include the date
            'duration': participant['duration']
        }

        # Add 'webinar_end_time' only for the first "Carl Helgesson"
        if participant['name'] == 'Carl Helgesson' and not carl_processed:
            processed_participant['webinar_end_time'] = adjusted_leave_time.strftime('%Y-%m-%d %I:%M %p')
            carl_processed = True

        processed_participants.append(processed_participant)

    return processed_participants

# Example usage
all_participants = [
    {
        'id': 'xFxT9TAYRfu1kuN99GJrig',
        'name': 'Carl Helgesson',
        'user_id': '16778240',
        'registrant_id': 'xFxT9TAYRfu1kuN99GJrig',
        'user_email': 'carl@rankonamazon.com',
        'join_time': datetime(2025, 3, 20, 18, 29, 16, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 15, 54, tzinfo=timezone.utc),
        'duration': 9998,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': True
    },
    {
        'id': '',
        'name': 'Peter Adehill',
        'user_id': '16779264',
        'registrant_id': '3QiwU14MSga2s4ComHliEg',
        'user_email': 'test2@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 18, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 19, 41, 38, tzinfo=timezone.utc),
        'duration': 4340,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Marlen Garamy',
        'user_id': '16780288',
        'registrant_id': 'PqUgpm6ZRQGmUASJOvMw8A',
        'user_email': 'test3@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 18, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 8, 48, tzinfo=timezone.utc),
        'duration': 5970,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Mariana',
        'user_id': '16781312',
        'registrant_id': 'qGL8EdjFTfqOBzwSTrmeBA',
        'user_email': 'test4@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 19, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 40, 26, tzinfo=timezone.utc),
        'duration': 7867,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Frida Wingman',
        'user_id': '16782336',
        'registrant_id': 'jQEhiLTNTc-qKcTGFtk9Yw',
        'user_email': 'test5@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 19, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 43, 11, tzinfo=timezone.utc),
        'duration': 8032,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Sejla B',
        'user_id': '16783360',
        'registrant_id': 'LPdDfWUAR9m5fvZZizme8A',
        'user_email': 'test6@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 20, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 26, 43, tzinfo=timezone.utc),
        'duration': 7043,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Maria S',
        'user_id': '16784384',
        'registrant_id': 'i7Vu70KVQ7i1gtpZI0oOKQ',
        'user_email': 'test7@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 20, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 19, 57, 55, tzinfo=timezone.utc),
        'duration': 5315,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Tessan Levy',
        'user_id': '16785408',
        'registrant_id': '8wqIm5iKTze9vAg0PDwejQ',
        'user_email': 'test8@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 20, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 23, 54, tzinfo=timezone.utc),
        'duration': 6874,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Johanna S',
        'user_id': '16786432',
        'registrant_id': 'iXwr-BU9QzGiiv39WR-OQw',
        'user_email': 'test9@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 21, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 45, 6, tzinfo=timezone.utc),
        'duration': 8145,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Olle,B',
        'user_id': '16787456',
        'registrant_id': 'HoGelA2WS_iCZa7hRVpGfQ',
        'user_email': 'test10@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 22, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 15, 54, tzinfo=timezone.utc),
        'duration': 9992,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Laila',
        'user_id': '16788480',
        'registrant_id': 'wccnhGtkQamEw8w2X-1DIg',
        'user_email': 'test11@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 23, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 13, 0, tzinfo=timezone.utc),
        'duration': 6217,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Emma',
        'user_id': '16789504',
        'registrant_id': 'vzh_asb1QCmIQUQAiTcXJg',
        'user_email': 'test12@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 24, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 15, 54, tzinfo=timezone.utc),
        'duration': 9990,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'vilje',
        'user_id': '16790528',
        'registrant_id': 'ZTL_1-A9TpqWAOoaV0kFAg',
        'user_email': 'test13@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 25, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 7, 33, tzinfo=timezone.utc),
        'duration': 9488,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Ulla',
        'user_id': '16791552',
        'registrant_id': '6USbRNPoTpGMu3vh_JeuAQ',
        'user_email': 'test14@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 26, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 19, 31, 41, tzinfo=timezone.utc),
        'duration': 3735,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Håkan Sundmark',
        'user_id': '16792576',
        'registrant_id': 'qxPfXN6TRMynAvQi_Ozr7A',
        'user_email': 'test15@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 26, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 42, 29, tzinfo=timezone.utc),
        'duration': 7983,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Dag Adler',
        'user_id': '16793600',
        'registrant_id': '4KW5hw9EQ6WjEaCVxIDwFQ',
        'user_email': 'test16@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 26, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 14, 23, tzinfo=timezone.utc),
        'duration': 9897,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Rannveig',
        'user_id': '16794624',
        'registrant_id': 'dasftvUMRkWYHbxFddXdHg',
        'user_email': 'test17@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 26, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 15, 53, tzinfo=timezone.utc),
        'duration': 9987,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Annika',
        'user_id': '16795648',
        'registrant_id': '82kofrX6S3qEX9xfhNgw5g',
        'user_email': 'test18@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 27, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 15, 54, tzinfo=timezone.utc),
        'duration': 9987,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'iPhone',
        'user_id': '16796672',
        'registrant_id': 'ffJQA052StOnaqyKzht8dA',
        'user_email': 'test19@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 27, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 19, 6, 54, tzinfo=timezone.utc),
        'duration': 2247,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Eleonor – iPhone',
        'user_id': '16797696',
        'registrant_id': '42gW3vngSbWpCWFxYLgORg',
        'user_email': 'test20@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 28, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 39, 29, tzinfo=timezone.utc),
        'duration': 7801,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Eva z',
        'user_id': '16820224',
        'registrant_id': 'h8AKFZtGQ2iFJcz8WU_okQ',
        'user_email': 'test21@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 47, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 19, 16, 26, tzinfo=timezone.utc),
        'duration': 2799,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Danny Nadler',
        'user_id': '16821248',
        'registrant_id': 'boXDHr3WQ6iVSaezlih5zA',
        'user_email': 'test22@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 48, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 1, 52, tzinfo=timezone.utc),
        'duration': 9124,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Edita Karlsson iPhone',
        'user_id': '16822272',
        'registrant_id': 'RxL2EtOnRVSzWnIrd9oidg',
        'user_email': 'test23@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 29, 49, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 18, 53, 22, tzinfo=timezone.utc),
        'duration': 1413,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Caroline Holmgren',
        'user_id': '16823296',
        'registrant_id': '6x0gKhLyQWWP3aM7_hP_sg',
        'user_email': 'test24@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 30, 4, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 15, 54, tzinfo=timezone.utc),
        'duration': 9950,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'tinatronestam',
        'user_id': '16824320',
        'registrant_id': 'oFgfcw20Ro-G1L16bX9csg',
        'user_email': 'test25@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 30, 10, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 15, 54, tzinfo=timezone.utc),
        'duration': 9944,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Johan Thunqvist',
        'user_id': '16825344',
        'registrant_id': '9vy4POqdQBaWTj6TJdvY2Q',
        'user_email': 'test26@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 30, 33, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 41, 43, tzinfo=timezone.utc),
        'duration': 7870,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Sara Amberin Danielsson',
        'user_id': '16826368',
        'registrant_id': 'GAoYKI97TfO67zFdBFlOtw',
        'user_email': 'test27@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 30, 39, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 32, 44, tzinfo=timezone.utc),
        'duration': 7325,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Mli',
        'user_id': '16827392',
        'registrant_id': 'fEBC8EHFSqK13YOsWJZEoQ',
        'user_email': 'test28@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 31, 45, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 14, 42, tzinfo=timezone.utc),
        'duration': 9777,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'iPhone',
        'user_id': '16828416',
        'registrant_id': 'Amn0W7V2RoqsNHeP45oQjA',
        'user_email': 'test29@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 31, 52, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 18, 48, 6, tzinfo=timezone.utc),
        'duration': 974,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Caroline Park',
        'user_id': '16829440',
        'registrant_id': 'TqKEYf8sShaHgc9Cw9vWNQ',
        'user_email': 'test30@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 32, 14, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 9, 19, tzinfo=timezone.utc),
        'duration': 5825,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Johan Hallberg',
        'user_id': '16830464',
        'registrant_id': 'HfSxnWYySJeo65tG8_0muA',
        'user_email': 'test31@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 32, 38, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 18, 32, 53, tzinfo=timezone.utc),
        'duration': 15,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Hassan Alarady',
        'user_id': '16831488',
        'registrant_id': 'CP51IgtSQsCEikxD9DAr7w',
        'user_email': 'test32@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 32, 43, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 18, 47, 37, tzinfo=timezone.utc),
        'duration': 894,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Johan Hallberg',
        'user_id': '16832512',
        'registrant_id': 'HfSxnWYySJeo65tG8_0muA',
        'user_email': 'test33@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 32, 54, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 15, 54, tzinfo=timezone.utc),
        'duration': 9780,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Marie',
        'user_id': '16833536',
        'registrant_id': 'pIkbWKnQQaCd_mhW_7r9vg',
        'user_email': 'test34@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 33, 2, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 3, 18, tzinfo=timezone.utc),
        'duration': 9016,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Alyonas iPhone',
        'user_id': '16834560',
        'registrant_id': 'SE-2Op08RgGlb9DrJukikw',
        'user_email': 'test35@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 33, 26, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 18, 37, 2, tzinfo=timezone.utc),
        'duration': 216,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Emira Zelenjakovic',
        'user_id': '16835584',
        'registrant_id': '4_kylRn5RIChxcacs3iD4A',
        'user_email': 'test36@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 33, 29, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 47, 50, tzinfo=timezone.utc),
        'duration': 8061,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Hilma Gustafsson',
        'user_id': '16836608',
        'registrant_id': 'QloHAI4xTBGAyBEnR18H7A',
        'user_email': 'test37@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 33, 31, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 20, 1, 42, tzinfo=timezone.utc),
        'duration': 5291,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'srisailam marupakula',
        'user_id': '16837632',
        'registrant_id': '-Rzs1l_VRwq8isdD2stuvw',
        'user_email': 'test38@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 33, 41, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 15, 53, tzinfo=timezone.utc),
        'duration': 9732,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Emma',
        'user_id': '16838656',
        'registrant_id': '_-xRwK9KStWaPRctTFTuJw',
        'user_email': 'test39@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 33, 49, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 9, 30, tzinfo=timezone.utc),
        'duration': 9341,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '',
        'name': 'Ilyas M Hussein',
        'user_id': '16839680',
        'registrant_id': 'xTHY-649SdOYmBpFa_HqFg',
        'user_email': 'test40@example.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 18, 33, 51, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 19, 12, 46, tzinfo=timezone.utc),
        'duration': 2335,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': False
    },
    {
        'id': '60HY96JhQra5lkM-FnXOwg',
        'name': 'Michael Kaiser',
        'user_id': '16877568',
        'registrant_id': '60HY96JhQra5lkM-FnXOwg',
        'user_email': 'mike@amanordic.com',  # Replaced with a test email
        'join_time': datetime(2025, 3, 20, 21, 9, 51, tzinfo=timezone.utc),
        'leave_time': datetime(2025, 3, 20, 21, 10, 11, tzinfo=timezone.utc),
        'duration': 20,
        'failover': False,
        'status': 'in_meeting',
        'internal_user': True
    }
]

# # Process the participants
# processed_participants = process_zoom_participants(all_participants)

# # Print the results
# print("Processed Participants:")
# for participant in processed_participants:
#     print(participant)