# GeoChat

GeoChat is a location-based social app that combines a live map, regional chat rooms, and short-lived meetup activities. Users can sign up, complete a public profile, discover nearby places, join a shared 5 km chat hub, and host activity rooms for other travelers nearby.

## Features

- Live 5 km regional chat rooms based on map location
- Short-lived activity rooms for meetups like coffee, sports, dinner, and more
- Account registration, login, JWT auth, and profile completion flow
- Friend search, friend requests, accepted-friend status, and live notifications
- Activity history and host quota tracking
- MongoDB persistence with automatic in-memory fallback when Mongo is unavailable
- Realtime messaging and presence updates with Socket.IO

## Tech Stack

- Node.js
- Express
- Socket.IO
- MongoDB with Mongoose
- Single-file frontend in `index.html`

## Project Structure

- `server.js` - Express server, auth, REST APIs, Socket.IO events, and storage logic
- `index.html` - Client UI, map interactions, auth screens, activity flows, and realtime chat
- `join.mp3` / `notify.mp3` - UI sound effects

## Getting Started

### 1. Install dependencies

```bash
npm install
```

### 2. Add a Google Maps API key

The frontend currently contains a placeholder key in `index.html`:

```js
const GOOGLE_MAPS_API_SRC = 'https://maps.googleapis.com/maps/api/js?key=YOURAPIKEY&libraries=places,visualization&callback=onGeoChatMapsReady';
```

Replace `YOURAPIKEY` with a valid Google Maps JavaScript API key that has Places and Visualization enabled.

### 3. Configure MongoDB

The server tries to connect to:

```text
mongodb://127.0.0.1:27017/Geochat_DB
```

If MongoDB is not running, the app still starts and falls back to in-memory storage. In that mode, data is lost when the server restarts.

### 4. Set environment variables

Optional environment variables:

- `PORT` - Defaults to `3000`
- `JWT_SECRET` - Defaults to `geochat-dev-secret-change-me`

Example:

```powershell
$env:PORT=3000
$env:JWT_SECRET="replace-this-secret"
npm start
```

### 5. Start the app

```bash
npm start
```

Then open [http://localhost:3000](http://localhost:3000).

## API Overview

### Auth

- `POST /api/auth/register`
- `POST /api/auth/login`
- `GET /api/auth/me`
- `PUT /api/auth/profile`

### Friends

- `GET /api/friends/summary`
- `GET /api/users/search?q=...`
- `POST /api/friends/requests`
- `POST /api/friends/requests/:relationshipId/accept`
- `POST /api/friends/notifications/read-all`

### Activities

- `GET /api/activities/hosting-status`
- `GET /api/activities/history`
- `POST /api/activities/:activityId/end`

## Notes

- The default JWT secret is fine for local development only.
- The frontend and styles are currently kept in a single HTML file, so UI changes will mostly happen in `index.html`.
- Activity and presence updates are powered by Socket.IO, so both REST and realtime flows are part of the app.
