import asyncio
import json
import os
import mimetypes
import random
import string

from websockets.asyncio.server import serve
from websockets.http11 import Response
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed

PORT = int(os.environ.get('PORT', 3000))
PUBLIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'public')

rooms = {}

def generate_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

async def signaling(websocket):
    peer_id = id(websocket)
    current_room = None

    try:
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get('type')

            if msg_type == 'create-room':
                room_id = generate_id()
                rooms[room_id] = {'host': websocket, 'host_id': peer_id}
                current_room = room_id
                await websocket.send(json.dumps({'type': 'room-created', 'id': room_id}))

            elif msg_type == 'join-room':
                room_id = data['id']
                room = rooms.get(room_id)
                if room and 'guest' not in room:
                    room['guest'] = websocket
                    room['guest_id'] = peer_id
                    current_room = room_id
                    await websocket.send(json.dumps({'type': 'room-joined', 'id': room_id}))
                    await room['host'].send(json.dumps({'type': 'peer-joined'}))
                else:
                    await websocket.send(json.dumps({
                        'type': 'room-error',
                        'msg': 'Room not found or full'
                    }))

            elif msg_type == 'signal':
                room_id = data['room']
                room = rooms.get(room_id)
                if room:
                    target = room['guest'] if room.get('host_id') == peer_id else room.get('host')
                    if target:
                        await target.send(json.dumps({
                            'type': 'signal',
                            'signalType': data['signalType'],
                            'data': data['data']
                        }))

            elif msg_type == 'leave':
                break

    except ConnectionClosed:
        pass
    finally:
        for room_id, room in list(rooms.items()):
            if room.get('host_id') == peer_id or room.get('guest_id') == peer_id:
                if 'guest' in room:
                    other = room['guest'] if room['host_id'] == peer_id else room['host']
                    try:
                        await other.send(json.dumps({'type': 'peer-left'}))
                    except ConnectionClosed:
                        pass
                del rooms[room_id]
                break

async def process_request(connection, request):
    path = request.path
    if path == '/ws':
        return None

    if path == '/':
        path = '/index.html'

    filepath = os.path.normpath(os.path.join(PUBLIC_DIR, path.lstrip('/')))

    if not filepath.startswith(PUBLIC_DIR) or not os.path.isfile(filepath):
        return Response(404, 'Not Found', Headers(), b'Not found')

    content_type, _ = mimetypes.guess_type(filepath)
    if content_type is None:
        content_type = 'application/octet-stream'

    with open(filepath, 'rb') as f:
        body = f.read()

    headers = Headers([
        ('Content-Type', content_type),
        ('Content-Length', str(len(body))),
    ])
    return Response(200, 'OK', headers, body)

async def main():
    async with serve(signaling, '0.0.0.0', PORT, process_request=process_request):
        print(f'Server running on http://localhost:{PORT}')
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())
