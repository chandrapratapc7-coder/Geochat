const express = require('express');
const http = require('http');
const path = require('path');
const { randomUUID } = require('crypto');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { Server } = require('socket.io');
const mongoose = require('mongoose');

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'geochat-dev-secret-change-me';
const JWT_EXPIRES_IN = '7d';
const HUB_RADIUS_METERS = 5000;
const HISTORY_LIMIT = 50;
const ACTIVITY_TTL_MS = 48 * 60 * 60 * 1000;
const ACTIVITY_CLEANUP_INTERVAL_MS = 5 * 60 * 1000;
const ACTIVITY_HOST_LIMIT = 2;
const ACTIVITY_HOST_WINDOW_MS = 24 * 60 * 60 * 1000;
const ACTIVITY_MIN_AGE = 18;
const ACTIVITY_MAX_AGE = 60;
const USER_SEARCH_LIMIT = 12;
const FRIEND_NOTIFICATION_LIMIT = 30;
const GENERIC_HUB_NAMES = new Set(['dropped pin', 'selected spot']);
const ALLOWED_GENDERS = new Set(['male', 'female', 'non_binary', 'prefer_not_to_say']);
const ACTIVITY_THEME_LABELS = {
    coffee: 'Coffee Meetup',
    wine: 'Wine Meetup',
    mocktail: 'Mocktail Meetup',
    sports: 'Turf & Sports Meetup',
    snooker: 'Snooker Meetup',
    fastfood: 'Fast Food Meetup',
    clubdrinks: 'Club&Drinks Meetup',
    swimming: 'Swimming Meetup',
    cafe: 'Cafe Exploring',
    dinner: 'Cafes & Restaurants Meetup',
    tourist: 'Travel Meetup',
    viewpoints: 'Sunset Points Meetup',
    cocktail: 'Cocktail Meetup'
};
const ACTIVITY_THEMES = new Set(Object.keys(ACTIVITY_THEME_LABELS));
const CREATABLE_ACTIVITY_THEMES = new Set([...ACTIVITY_THEMES].filter((theme) => theme !== 'wine' && theme !== 'mocktail' && theme !== 'viewpoints' && theme !== 'cafe'));

mongoose.set('bufferCommands', false);

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const memoryStore = {
    hubs: [],
    activities: [],
    messages: [],
    sessions: [],
    users: [],
    relationships: [],
    notifications: []
};

let storageMode = 'memory';

const messageSchema = new mongoose.Schema({
    room: String,
    roomName: String,
    user: String,
    userId: String,
    text: String,
    time: String,
    timestamp: { type: Date, default: Date.now, expires: 86400 }
});

const sessionSchema = new mongoose.Schema({
    socketId: String,
    username: String,
    userId: String,
    profileId: String,
    placeId: String,
    roomName: String,
    roomType: String,
    lat: Number,
    lng: Number,
    joinedAt: { type: Date, default: Date.now }
});

const hubSchema = new mongoose.Schema({
    name: String,
    location: {
        type: { type: String, default: 'Point' },
        coordinates: [Number]
    },
    createdAt: { type: Date, default: Date.now }
});

const activitySchema = new mongoose.Schema({
    host: String,
    hostId: String,
    goal: String,
    theme: String,
    scheduledFor: { type: String, default: '' },
    ageMin: { type: Number, default: ACTIVITY_MIN_AGE },
    ageMax: { type: Number, default: ACTIVITY_MAX_AGE },
    isExpired: { type: Boolean, default: false },
    endedAt: { type: Date, default: null },
    endedByHost: { type: Boolean, default: false },
    placeName: String,
    placeKey: String,
    vicinity: String,
    participants: { type: [String], default: [] },
    participantIds: { type: [String], default: [] },
    location: {
        type: { type: String, default: 'Point' },
        coordinates: [Number]
    },
    createdAt: { type: Date, default: Date.now }
});

const userSchema = new mongoose.Schema({
    profileId: { type: String, default: () => randomUUID() },
    auth: {
        username: { type: String, required: true, trim: true, lowercase: true },
        passwordHash: { type: String, required: true }
    },
    profile: {
        name: { type: String, default: '' },
        birthDate: { type: Date, default: null },
        gender: { type: String, default: '' },
        bio: { type: String, default: '' },
        occupation: { type: String, default: '' },
        avatarUrl: { type: String, default: '' }
    },
    isProfileComplete: { type: Boolean, default: false },
    lastSeen: { type: Date, default: Date.now },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now }
});

const relationshipSchema = new mongoose.Schema({
    pairKey: { type: String, required: true },
    requesterId: { type: String, required: true },
    recipientId: { type: String, required: true },
    status: { type: String, enum: ['pending', 'accepted'], default: 'pending' },
    acceptedAt: { type: Date, default: null },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now }
});

const notificationSchema = new mongoose.Schema({
    userId: { type: String, required: true },
    actorId: { type: String, default: '' },
    type: { type: String, required: true },
    title: { type: String, default: '' },
    body: { type: String, default: '' },
    roomId: { type: String, default: '' },
    roomType: { type: String, default: '' },
    activityId: { type: String, default: '' },
    isRead: { type: Boolean, default: false },
    createdAt: { type: Date, default: Date.now }
});

hubSchema.index({ location: '2dsphere' });
activitySchema.index({ location: '2dsphere' });
activitySchema.index({ hostId: 1, createdAt: -1 });
userSchema.index({ 'auth.username': 1 }, { unique: true });
userSchema.index({ profileId: 1 }, { unique: true });
relationshipSchema.index({ pairKey: 1 }, { unique: true });
relationshipSchema.index({ requesterId: 1, status: 1, createdAt: -1 });
relationshipSchema.index({ recipientId: 1, status: 1, createdAt: -1 });
notificationSchema.index({ userId: 1, createdAt: -1 });
notificationSchema.index({ userId: 1, isRead: 1, createdAt: -1 });

const Hub = mongoose.model('Hub', hubSchema);
const Activity = mongoose.model('Activity', activitySchema);
const Message = mongoose.model('Message', messageSchema);
const Session = mongoose.model('Session', sessionSchema);
const User = mongoose.model('User', userSchema);
const Relationship = mongoose.model('Relationship', relationshipSchema);
const FriendNotification = mongoose.model('FriendNotification', notificationSchema);

app.use(express.json({ limit: '1mb' }));

app.get(['/join.mp3', '/notify.mp3'], (req, res) => {
    res.sendFile(path.join(__dirname, req.path.slice(1)));
});

app.get(['/', '/index.html', '/login', '/signup', '/complete-profile', '/activity-history'], (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

const liveOccupancy = {};
const pendingActivityCreations = new Set();

function usingMongoStore() {
    return storageMode === 'mongo';
}

function buildMemoryDocument(data) {
    return {
        _id: randomUUID(),
        createdAt: new Date(),
        updatedAt: new Date(),
        ...data
    };
}

function toTimestamp(value) {
    if (value instanceof Date) return value.getTime();
    return new Date(value).getTime();
}

function normalizeUsername(value) {
    return String(value || '').trim().toLowerCase();
}

function trimToLength(value, maxLength) {
    return String(value || '').trim().slice(0, maxLength);
}

function escapeRegex(value) {
    return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function buildRelationshipPairKey(leftUserId, rightUserId) {
    const left = String(leftUserId || '').trim();
    const right = String(rightUserId || '').trim();
    if (!left || !right) return '';
    return [left, right].sort().join(':');
}

function getUserSocketRoom(userId) {
    return `user:${String(userId || '').trim()}`;
}

function normalizeActivityDateKey(value) {
    const raw = trimToLength(value, 32);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return '';
    const [yearValue, monthValue, dayValue] = raw.split('-');
    const year = Number(yearValue);
    const month = Number(monthValue);
    const day = Number(dayValue);
    const parsed = new Date(Date.UTC(year, month - 1, day));
    if (Number.isNaN(parsed.getTime())) return '';
    if (parsed.getUTCFullYear() !== year || parsed.getUTCMonth() !== month - 1 || parsed.getUTCDate() !== day) return '';
    return raw;
}

function normalizeActivityAgeValue(value) {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed)) return null;
    return Math.max(ACTIVITY_MIN_AGE, Math.min(ACTIVITY_MAX_AGE, parsed));
}

function buildActivityAgeRange(minValue, maxValue) {
    const parsedMin = normalizeActivityAgeValue(minValue);
    const parsedMax = normalizeActivityAgeValue(maxValue);
    if (!Number.isFinite(parsedMin) || !Number.isFinite(parsedMax)) {
        return { min: ACTIVITY_MIN_AGE, max: ACTIVITY_MAX_AGE };
    }
    return parsedMin <= parsedMax
        ? { min: parsedMin, max: parsedMax }
        : { min: parsedMax, max: parsedMin };
}

function parseBirthDate(value) {
    const raw = String(value || '').trim();
    if (!raw) return null;
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) return null;
    if (parsed > new Date()) return null;
    return parsed;
}

function calculateAge(birthDate) {
    if (!birthDate) return null;
    const date = birthDate instanceof Date ? birthDate : new Date(birthDate);
    if (Number.isNaN(date.getTime())) return null;
    const now = new Date();
    let age = now.getFullYear() - date.getFullYear();
    const beforeBirthday = now.getMonth() < date.getMonth()
        || (now.getMonth() === date.getMonth() && now.getDate() < date.getDate());
    if (beforeBirthday) age -= 1;
    return age >= 0 ? age : null;
}

function normalizeThemeKey(theme) {
    const normalizedTheme = String(theme || '').trim().toLowerCase();
    if (normalizedTheme === 'clubs' || normalizedTheme === 'drinking') return 'clubdrinks';
    return normalizedTheme;
}

function getValidTheme(theme, allowedThemes = ACTIVITY_THEMES) {
    const normalizedTheme = normalizeThemeKey(theme);
    return allowedThemes.has(normalizedTheme) ? normalizedTheme : null;
}

function sanitizeTheme(theme, allowedThemes = ACTIVITY_THEMES) {
    return getValidTheme(theme, allowedThemes) || 'coffee';
}

function getThemeLabel(theme) {
    const validTheme = getValidTheme(theme);
    return validTheme ? ACTIVITY_THEME_LABELS[validTheme] : 'Unknown Activity';
}

function formatDurationCompact(ms) {
    const totalMinutes = Math.max(1, Math.ceil(Math.max(0, Number(ms) || 0) / 60000));
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;

    if (hours >= 24) {
        const days = Math.floor(hours / 24);
        const remainingHours = hours % 24;
        if (remainingHours) return `${days}d ${remainingHours}h`;
        return `${days}d`;
    }

    if (hours && minutes) return `${hours}h ${minutes}m`;
    if (hours) return `${hours}h`;
    return `${totalMinutes}m`;
}

function buildActivityHostQuotaStatus(activities, now = new Date()) {
    const nowMs = now instanceof Date ? now.getTime() : Number(now);
    const sortedActivities = [...(activities || [])].sort((left, right) => toTimestamp(left.createdAt) - toTimestamp(right.createdAt));
    const used = sortedActivities.length;
    const remaining = Math.max(0, ACTIVITY_HOST_LIMIT - used);
    let nextAvailableAt = null;
    let retryAfterMs = 0;

    if (used >= ACTIVITY_HOST_LIMIT) {
        const unlockIndex = used - ACTIVITY_HOST_LIMIT;
        const unlockAtMs = toTimestamp(sortedActivities[unlockIndex].createdAt) + ACTIVITY_HOST_WINDOW_MS;
        retryAfterMs = Math.max(0, unlockAtMs - nowMs);
        nextAvailableAt = new Date(unlockAtMs).toISOString();
    }

    return {
        limit: ACTIVITY_HOST_LIMIT,
        windowMs: ACTIVITY_HOST_WINDOW_MS,
        used,
        remaining,
        isLimited: used >= ACTIVITY_HOST_LIMIT,
        retryAfterMs,
        nextAvailableAt
    };
}

function isActivityManuallyOrSoftExpired(activity) {
    const source = activity && activity.toObject ? activity.toObject() : activity;
    return Boolean(source && source.isExpired);
}

function buildHubName(preferredName, lat, lng) {
    const cleanName = typeof preferredName === 'string' ? preferredName.trim() : '';
    if (cleanName && !GENERIC_HUB_NAMES.has(cleanName.toLowerCase())) {
        return cleanName.slice(0, 80);
    }
    return `Area ${lat.toFixed(3)}, ${lng.toFixed(3)}`;
}

function distanceMeters(lat1, lng1, lat2, lng2) {
    const toRadians = (degrees) => (degrees * Math.PI) / 180;
    const earthRadius = 6371000;
    const latDelta = toRadians(lat2 - lat1);
    const lngDelta = toRadians(lng2 - lng1);
    const a = Math.sin(latDelta / 2) ** 2
        + Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(lngDelta / 2) ** 2;
    return 2 * earthRadius * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function isValidAvatarUrl(value) {
    try {
        const parsed = new URL(String(value || '').trim());
        return parsed.protocol === 'http:' || parsed.protocol === 'https:';
    } catch (error) {
        return false;
    }
}

function computeProfileComplete(profile) {
    return Boolean(
        profile
        && trimToLength(profile.name, 80)
        && profile.birthDate
        && ALLOWED_GENDERS.has(profile.gender)
        && trimToLength(profile.bio, 240)
        && trimToLength(profile.occupation, 80)
        && trimToLength(profile.avatarUrl, 400)
    );
}

function buildUserProfilePayload(payload = {}) {
    const name = trimToLength(payload.name, 80);
    const bio = trimToLength(payload.bio, 240);
    const occupation = trimToLength(payload.occupation, 80);
    const avatarUrl = trimToLength(payload.avatarUrl, 400);
    const gender = trimToLength(payload.gender, 32);
    const birthDate = parseBirthDate(payload.birthDate);

    if (!name) return { error: 'Add your full name.' };
    if (!birthDate) return { error: 'Add a valid birth date.' };
    if (!ALLOWED_GENDERS.has(gender)) return { error: 'Choose a valid gender option.' };
    if (!occupation) return { error: 'Add your occupation.' };
    if (!bio) return { error: 'Add a short bio.' };
    if (!avatarUrl || !isValidAvatarUrl(avatarUrl)) return { error: 'Add a valid profile picture URL.' };

    return {
        value: {
            name,
            birthDate,
            gender,
            bio,
            occupation,
            avatarUrl
        }
    };
}

function serializeUser(user) {
    const source = user.toObject ? user.toObject() : user;
    const birthDate = source.profile && source.profile.birthDate ? new Date(source.profile.birthDate) : null;
    return {
        id: String(source._id),
        profileId: source.profileId,
        username: source.auth.username,
        name: source.profile.name || source.auth.username,
        birthDate: birthDate ? birthDate.toISOString().slice(0, 10) : '',
        age: calculateAge(birthDate),
        gender: source.profile.gender || '',
        bio: source.profile.bio || '',
        occupation: source.profile.occupation || '',
        avatarUrl: source.profile.avatarUrl || '',
        isProfileComplete: Boolean(source.isProfileComplete),
        lastSeen: source.lastSeen,
        createdAt: source.createdAt
    };
}

function serializePublicUser(user) {
    const source = user.toObject ? user.toObject() : user;
    const birthDate = source.profile && source.profile.birthDate ? new Date(source.profile.birthDate) : null;
    return {
        id: String(source._id),
        profileId: source.profileId,
        username: source.auth.username,
        name: source.profile.name || source.auth.username,
        age: calculateAge(birthDate),
        gender: source.profile.gender || '',
        bio: source.profile.bio || '',
        occupation: source.profile.occupation || '',
        avatarUrl: source.profile.avatarUrl || '',
        isProfileComplete: Boolean(source.isProfileComplete),
        lastSeen: source.lastSeen,
        createdAt: source.createdAt
    };
}

function getDisplayName(user) {
    const source = user.toObject ? user.toObject() : user;
    return trimToLength(source.profile && source.profile.name ? source.profile.name : source.auth.username, 80) || 'Traveler';
}

function createToken(user) {
    const source = user.toObject ? user.toObject() : user;
    return jwt.sign(
        {
            sub: String(source._id),
            username: source.auth.username,
            profileId: source.profileId
        },
        JWT_SECRET,
        { expiresIn: JWT_EXPIRES_IN }
    );
}

function serializeActivity(activity) {
    const source = activity.toObject ? activity.toObject() : activity;
    const coordinates = source.location && Array.isArray(source.location.coordinates)
        ? source.location.coordinates
        : [0, 0];
    const ageRange = buildActivityAgeRange(source.ageMin, source.ageMax);

    return {
        id: String(source._id),
        host: source.host,
        hostId: source.hostId || '',
        goal: source.goal,
        theme: getValidTheme(source.theme) || '',
        scheduledFor: normalizeActivityDateKey(source.scheduledFor),
        ageMin: ageRange.min,
        ageMax: ageRange.max,
        isExpired: Boolean(source.isExpired),
        endedAt: source.endedAt ? new Date(source.endedAt).toISOString() : '',
        endedByHost: Boolean(source.endedByHost),
        placeName: source.placeName,
        placeKey: source.placeKey,
        vicinity: source.vicinity,
        participants: source.participants || [],
        participantIds: source.participantIds || [],
        participantCount: (source.participants || []).length,
        lat: coordinates[1],
        lng: coordinates[0],
        createdAt: source.createdAt
    };
}

function serializeChatMessage(message) {
    const source = message && message.toObject ? message.toObject() : (message || {});
    return {
        room: source.room || '',
        roomName: source.roomName || '',
        user: source.user || '',
        userId: source.userId || '',
        text: source.text || '',
        time: source.time || ''
    };
}

function isActivityExpired(activity) {
    if (!activity || !activity.createdAt) return false;
    const createdAt = activity.createdAt instanceof Date ? activity.createdAt : new Date(activity.createdAt);
    if (Number.isNaN(createdAt.getTime())) return false;
    return Date.now() - createdAt.getTime() >= ACTIVITY_TTL_MS;
}

async function expireActivityRecord(activity, options = {}) {
    if (!activity || isActivityManuallyOrSoftExpired(activity)) return activity;

    activity.isExpired = true;
    activity.endedAt = options.endedAt instanceof Date ? options.endedAt : new Date();
    activity.endedByHost = Boolean(options.endedByHost);
    await saveActivityRecord(activity);
    clearLiveOccupancy([String(activity._id)]);
    return activity;
}

function clearLiveOccupancy(roomIds) {
    roomIds.forEach((roomId) => {
        delete liveOccupancy[String(roomId)];
    });
}

async function pruneExpiredActivities() {
    const cutoff = new Date(Date.now() - ACTIVITY_TTL_MS);

    if (usingMongoStore()) {
        const expired = await Activity.find({
            createdAt: { $lte: cutoff },
            isExpired: { $ne: true }
        }).select('_id hostId').lean();
        if (!expired.length) return [];
        const expiredIds = expired.map((activity) => String(activity._id));
        await Activity.updateMany(
            { _id: { $in: expiredIds } },
            { $set: { isExpired: true, endedAt: new Date(), endedByHost: false } }
        );
        clearLiveOccupancy(expiredIds);
        await Promise.all(expired.map((activity) => closeActivityRoom(activity._id, {
            activityId: String(activity._id),
            hostId: String(activity.hostId || ''),
            endedByHost: false
        })));
        return expiredIds;
    }

    const expiredIds = [];
    memoryStore.activities.forEach((activity) => {
        if (!isActivityManuallyOrSoftExpired(activity) && isActivityExpired(activity)) {
            activity.isExpired = true;
            activity.endedAt = new Date();
            activity.endedByHost = false;
            activity.updatedAt = new Date();
            expiredIds.push(String(activity._id));
        }
    });

    if (!expiredIds.length) return [];

    clearLiveOccupancy(expiredIds);
    await Promise.all(expiredIds.map((activityId) => {
        const activity = memoryStore.activities.find((entry) => String(entry._id) === String(activityId));
        return closeActivityRoom(activityId, {
            activityId: String(activityId),
            hostId: String(activity && activity.hostId || ''),
            endedByHost: false
        });
    }));
    return expiredIds;
}

async function listActivities() {
    await pruneExpiredActivities();
    if (usingMongoStore()) return Activity.find({ isExpired: { $ne: true } }).sort({ createdAt: -1 });
    return memoryStore.activities
        .filter((activity) => !isActivityManuallyOrSoftExpired(activity))
        .sort((left, right) => toTimestamp(right.createdAt) - toTimestamp(left.createdAt));
}

async function listRecentActivitiesByHost(hostId, sinceDate) {
    const normalizedHostId = String(hostId || '');
    if (!normalizedHostId) return [];

    if (usingMongoStore()) {
        return Activity.find({
            hostId: normalizedHostId,
            isExpired: { $ne: true },
            createdAt: { $gte: sinceDate }
        }).select('createdAt').sort({ createdAt: 1 }).lean();
    }

    return memoryStore.activities
        .filter((activity) => (
            String(activity.hostId || '') === normalizedHostId
            && !isActivityManuallyOrSoftExpired(activity)
            && toTimestamp(activity.createdAt) >= sinceDate.getTime()
        ))
        .sort((left, right) => toTimestamp(left.createdAt) - toTimestamp(right.createdAt));
}

async function getActivityHostQuotaStatus(hostId, now = new Date()) {
    const nowDate = now instanceof Date ? now : new Date(now);
    const sinceDate = new Date(nowDate.getTime() - ACTIVITY_HOST_WINDOW_MS);
    const activities = await listRecentActivitiesByHost(hostId, sinceDate);
    return buildActivityHostQuotaStatus(activities, nowDate);
}

async function createActivityRecord(data) {
    if (usingMongoStore()) return Activity.create(data);
    const activity = buildMemoryDocument(data);
    memoryStore.activities.push(activity);
    return activity;
}

async function findStoredActivityRecord(activityId) {
    if (!activityId) return null;
    if (usingMongoStore()) return Activity.findById(activityId);
    return memoryStore.activities.find((entry) => String(entry._id) === String(activityId)) || null;
}

async function findActivityRecord(activityId) {
    const activity = await findStoredActivityRecord(activityId);
    if (!activity) return null;
    if (isActivityManuallyOrSoftExpired(activity)) return null;
    if (isActivityExpired(activity)) {
        await expireActivityRecord(activity, { endedAt: new Date(), endedByHost: false });
        await closeActivityRoom(activityId, {
            activityId: String(activityId),
            endedByHost: false
        });
        return null;
    }
    return activity;
}

async function saveActivityRecord(activity) {
    if (usingMongoStore()) {
        await activity.save();
        return activity;
    }

    const index = memoryStore.activities.findIndex((entry) => String(entry._id) === String(activity._id));
    const nextValue = { ...activity, updatedAt: new Date() };
    if (index >= 0) memoryStore.activities[index] = nextValue;
    else memoryStore.activities.push(nextValue);
    return nextValue;
}

async function deleteActivityRecord(activityId) {
    if (usingMongoStore()) {
        await Activity.deleteOne({ _id: activityId });
        return;
    }
    memoryStore.activities = memoryStore.activities.filter((activity) => String(activity._id) !== String(activityId));
}

async function listHostedActivityHistory(hostId) {
    await pruneExpiredActivities();
    const normalizedHostId = String(hostId || '');
    if (!normalizedHostId) return [];

    if (usingMongoStore()) {
        return Activity.find({
            hostId: normalizedHostId,
            isExpired: true
        }).sort({ endedAt: -1, createdAt: -1 });
    }

    return memoryStore.activities
        .filter((activity) => String(activity.hostId || '') === normalizedHostId && isActivityManuallyOrSoftExpired(activity))
        .sort((left, right) => {
            const leftEndedAt = left.endedAt ? toTimestamp(left.endedAt) : toTimestamp(left.createdAt);
            const rightEndedAt = right.endedAt ? toTimestamp(right.endedAt) : toTimestamp(right.createdAt);
            return rightEndedAt - leftEndedAt;
        });
}

async function listMessagesByRoom(roomId) {
    if (usingMongoStore()) return Message.find({ room: roomId }).sort({ timestamp: 1 }).limit(HISTORY_LIMIT);
    return memoryStore.messages
        .filter((message) => message.room === roomId)
        .sort((left, right) => toTimestamp(left.timestamp) - toTimestamp(right.timestamp))
        .slice(-HISTORY_LIMIT);
}

async function createMessageRecord(message) {
    if (usingMongoStore()) {
        await Message.create(message);
        return;
    }
    memoryStore.messages.push({ ...message, timestamp: new Date() });
}

async function upsertSessionRecord(sessionData) {
    if (usingMongoStore()) {
        await Session.findOneAndUpdate(
            { socketId: sessionData.socketId },
            sessionData,
            { upsert: true, setDefaultsOnInsert: true }
        );
        return;
    }

    const index = memoryStore.sessions.findIndex((session) => session.socketId === sessionData.socketId);
    if (index >= 0) {
        memoryStore.sessions[index] = { ...memoryStore.sessions[index], ...sessionData };
    } else {
        memoryStore.sessions.push({ ...sessionData, joinedAt: new Date() });
    }
}

async function deleteSessionRecord(socketId) {
    if (usingMongoStore()) {
        await Session.deleteOne({ socketId });
        return;
    }
    memoryStore.sessions = memoryStore.sessions.filter((session) => session.socketId !== socketId);
}

async function listSessionsByRoom(roomId) {
    if (usingMongoStore()) return Session.find({ placeId: roomId });
    return memoryStore.sessions.filter((session) => session.placeId === roomId);
}

async function findUserByUsername(username) {
    const normalized = normalizeUsername(username);
    if (!normalized) return null;
    if (usingMongoStore()) return User.findOne({ 'auth.username': normalized });
    return memoryStore.users.find((user) => user.auth && user.auth.username === normalized) || null;
}

async function findUserById(userId) {
    if (!userId) return null;
    if (usingMongoStore()) {
        try {
            return await User.findById(userId);
        } catch (error) {
            return null;
        }
    }
    return memoryStore.users.find((user) => String(user._id) === String(userId)) || null;
}

async function createUserRecord(data) {
    if (usingMongoStore()) return User.create(data);
    const user = buildMemoryDocument({
        profileId: randomUUID(),
        profile: {
            name: '',
            birthDate: null,
            gender: '',
            bio: '',
            occupation: '',
            avatarUrl: ''
        },
        isProfileComplete: false,
        lastSeen: new Date(),
        ...data
    });
    memoryStore.users.push(user);
    return user;
}

async function saveUserRecord(user) {
    if (usingMongoStore()) {
        user.updatedAt = new Date();
        await user.save();
        return user;
    }

    const index = memoryStore.users.findIndex((entry) => String(entry._id) === String(user._id));
    const nextValue = { ...user, updatedAt: new Date() };
    if (index >= 0) memoryStore.users[index] = nextValue;
    else memoryStore.users.push(nextValue);
    return nextValue;
}

async function touchUserRecord(user) {
    if (!user) return user;
    user.lastSeen = new Date();
    return saveUserRecord(user);
}

async function findUsersByIds(userIds) {
    const uniqueIds = [...new Set((userIds || []).map((userId) => String(userId || '')).filter(Boolean))];
    if (!uniqueIds.length) return [];
    if (usingMongoStore()) return User.find({ _id: { $in: uniqueIds } });
    return memoryStore.users.filter((user) => uniqueIds.includes(String(user._id)));
}

async function searchUsers(query, excludeUserId, limit = USER_SEARCH_LIMIT) {
    const cleanQuery = trimToLength(query, 80);
    if (cleanQuery.length < 2) return [];

    const normalizedQuery = cleanQuery.toLowerCase();
    const normalizedExcludeId = String(excludeUserId || '');

    if (usingMongoStore()) {
        const pattern = new RegExp(escapeRegex(cleanQuery), 'i');
        return User.find({
            _id: { $ne: normalizedExcludeId },
            isProfileComplete: true,
            $or: [
                { 'auth.username': pattern },
                { 'profile.name': pattern },
                { profileId: pattern }
            ]
        })
            .sort({ lastSeen: -1, createdAt: -1 })
            .limit(limit);
    }

    return memoryStore.users
        .filter((user) => {
            if (String(user._id) === normalizedExcludeId) return false;
            if (!user.isProfileComplete) return false;
            const haystack = [
                user.auth && user.auth.username,
                user.profile && user.profile.name,
                user.profileId
            ].map((value) => String(value || '').toLowerCase());
            return haystack.some((value) => value.includes(normalizedQuery));
        })
        .sort((left, right) => toTimestamp(right.lastSeen) - toTimestamp(left.lastSeen))
        .slice(0, limit);
}

async function findRelationshipById(relationshipId) {
    if (!relationshipId) return null;
    if (usingMongoStore()) {
        try {
            return await Relationship.findById(relationshipId);
        } catch (error) {
            return null;
        }
    }
    return memoryStore.relationships.find((relationship) => String(relationship._id) === String(relationshipId)) || null;
}

async function findRelationshipByPair(leftUserId, rightUserId) {
    const pairKey = buildRelationshipPairKey(leftUserId, rightUserId);
    if (!pairKey) return null;
    if (usingMongoStore()) return Relationship.findOne({ pairKey });
    return memoryStore.relationships.find((relationship) => relationship.pairKey === pairKey) || null;
}

async function listRelationshipsForUser(userId) {
    const normalizedUserId = String(userId || '');
    if (!normalizedUserId) return [];
    if (usingMongoStore()) {
        return Relationship.find({
            $or: [
                { requesterId: normalizedUserId },
                { recipientId: normalizedUserId }
            ]
        }).sort({ createdAt: -1 });
    }
    return memoryStore.relationships
        .filter((relationship) => (
            String(relationship.requesterId) === normalizedUserId
            || String(relationship.recipientId) === normalizedUserId
        ))
        .sort((left, right) => toTimestamp(right.createdAt) - toTimestamp(left.createdAt));
}

async function createRelationshipRecord(data) {
    const payload = { ...data, updatedAt: new Date() };
    if (usingMongoStore()) return Relationship.create(payload);
    const relationship = buildMemoryDocument(payload);
    memoryStore.relationships.push(relationship);
    return relationship;
}

async function saveRelationshipRecord(relationship) {
    if (usingMongoStore()) {
        relationship.updatedAt = new Date();
        await relationship.save();
        return relationship;
    }

    const index = memoryStore.relationships.findIndex((entry) => String(entry._id) === String(relationship._id));
    const nextValue = { ...relationship, updatedAt: new Date() };
    if (index >= 0) memoryStore.relationships[index] = nextValue;
    else memoryStore.relationships.push(nextValue);
    return nextValue;
}

async function listAcceptedFriendIds(userId) {
    const relationships = await listRelationshipsForUser(userId);
    const normalizedUserId = String(userId || '');
    return relationships
        .filter((relationship) => relationship.status === 'accepted')
        .map((relationship) => (
            String(relationship.requesterId) === normalizedUserId
                ? String(relationship.recipientId)
                : String(relationship.requesterId)
        ))
        .filter(Boolean);
}

function buildFriendStateFromRelationships(relationships, userId) {
    const normalizedUserId = String(userId || '');
    const acceptedFriendIds = [];
    const outgoingFriendRequestIds = [];

    (relationships || []).forEach((relationship) => {
        const requesterId = String(relationship.requesterId || '');
        const recipientId = String(relationship.recipientId || '');
        const counterpartId = requesterId === normalizedUserId ? recipientId : requesterId;
        if (!counterpartId) return;

        if (relationship.status === 'accepted') {
            acceptedFriendIds.push(counterpartId);
            return;
        }

        if (relationship.status === 'pending' && requesterId === normalizedUserId) {
            outgoingFriendRequestIds.push(counterpartId);
        }
    });

    return {
        acceptedFriendIds: [...new Set(acceptedFriendIds)],
        outgoingFriendRequestIds: [...new Set(outgoingFriendRequestIds)]
    };
}

function buildRoomParticipantSummary(session, acceptedFriendIdSet = new Set(), outgoingFriendRequestIdSet = new Set(), currentUserId = '') {
    const userId = String(session && session.userId || '').trim();
    const normalizedCurrentUserId = String(currentUserId || '').trim();
    const profileId = String(session && session.profileId || '').trim();
    const name = trimToLength(session && session.username, 80) || 'Traveler';
    const isCurrentUser = Boolean(userId) && userId === normalizedCurrentUserId;

    return {
        userId,
        profileId,
        name,
        isCurrentUser,
        isFriend: Boolean(userId) && !isCurrentUser && acceptedFriendIdSet.has(userId),
        hasOutgoingRequest: Boolean(userId) && !isCurrentUser && outgoingFriendRequestIdSet.has(userId)
    };
}

function buildActivityParticipantSummaries(activity, acceptedFriendIdSet = new Set(), outgoingFriendRequestIdSet = new Set(), currentUserId = '') {
    const source = activity && activity.toObject ? activity.toObject() : (activity || {});
    const participantIds = Array.isArray(source.participantIds) ? source.participantIds : [];
    const participants = Array.isArray(source.participants) ? source.participants : [];
    const total = Math.min(participantIds.length, participants.length);

    return Array.from({ length: total }, (_, index) => {
        const userId = String(participantIds[index] || '').trim();
        const name = trimToLength(participants[index], 80) || 'Traveler';
        const isCurrentUser = Boolean(userId) && userId === String(currentUserId || '').trim();
        return {
            userId,
            profileId: '',
            name,
            isCurrentUser,
            isFriend: Boolean(userId) && !isCurrentUser && acceptedFriendIdSet.has(userId),
            hasOutgoingRequest: Boolean(userId) && !isCurrentUser && outgoingFriendRequestIdSet.has(userId)
        };
    }).filter((entry) => entry.userId);
}

async function createFriendNotificationRecord(data) {
    if (usingMongoStore()) return FriendNotification.create(data);
    const notification = buildMemoryDocument(data);
    memoryStore.notifications.push(notification);
    return notification;
}

async function listFriendNotificationsByUser(userId, limit = FRIEND_NOTIFICATION_LIMIT) {
    const normalizedUserId = String(userId || '');
    if (!normalizedUserId) return [];
    if (usingMongoStore()) {
        return FriendNotification.find({ userId: normalizedUserId })
            .sort({ createdAt: -1 })
            .limit(limit);
    }
    return memoryStore.notifications
        .filter((notification) => String(notification.userId) === normalizedUserId)
        .sort((left, right) => toTimestamp(right.createdAt) - toTimestamp(left.createdAt))
        .slice(0, limit);
}

async function countUnreadFriendNotifications(userId) {
    const normalizedUserId = String(userId || '');
    if (!normalizedUserId) return 0;
    if (usingMongoStore()) return FriendNotification.countDocuments({ userId: normalizedUserId, isRead: false });
    return memoryStore.notifications.filter((notification) => String(notification.userId) === normalizedUserId && !notification.isRead).length;
}

async function markFriendNotificationsRead(userId) {
    const normalizedUserId = String(userId || '');
    if (!normalizedUserId) return;
    if (usingMongoStore()) {
        await FriendNotification.updateMany(
            { userId: normalizedUserId, isRead: false },
            { $set: { isRead: true } }
        );
        return;
    }

    memoryStore.notifications = memoryStore.notifications.map((notification) => {
        if (String(notification.userId) !== normalizedUserId || notification.isRead) return notification;
        return { ...notification, isRead: true, updatedAt: new Date() };
    });
}

function collectSocketPresenceByUserId() {
    const presenceMap = new Map();

    Array.from(io.sockets.sockets.values()).forEach((socket) => {
        const userId = String(socket.currentUserId || socket.authUserId || '').trim();
        if (!userId) return;
        if (!presenceMap.has(userId)) presenceMap.set(userId, []);
        presenceMap.get(userId).push({
            roomId: socket.currentRoom || '',
            roomName: socket.currentRoomName || '',
            roomType: socket.currentRoomType || ''
        });
    });

    return presenceMap;
}

function buildFriendPresenceStatus(user, presenceEntries = [], hostedActivity = null) {
    if (hostedActivity) {
        const activityId = String(hostedActivity._id || hostedActivity.id || '');
        const placeName = trimToLength(hostedActivity.placeName, 80);
        return {
            code: 'hosting_activity',
            label: placeName
                ? `Hosting ${getThemeLabel(hostedActivity.theme)} at ${placeName}`
                : `Hosting ${getThemeLabel(hostedActivity.theme)}`,
            isOnline: true,
            roomId: activityId,
            roomType: 'activity',
            activityId
        };
    }

    const activityPresence = presenceEntries.find((entry) => entry.roomType === 'activity');
    if (activityPresence) {
        return {
            code: 'in_activity',
            label: activityPresence.roomName
                ? `In activity: ${trimToLength(activityPresence.roomName, 80)}`
                : 'In an activity room',
            isOnline: true,
            roomId: activityPresence.roomId,
            roomType: 'activity',
            activityId: activityPresence.roomId
        };
    }

    const regionalPresence = presenceEntries.find((entry) => entry.roomType === 'regional');
    if (regionalPresence) {
        return {
            code: 'in_regional_chat',
            label: regionalPresence.roomName
                ? `In the 5 km chat near ${trimToLength(regionalPresence.roomName, 80)}`
                : 'In the 5 km regional chat',
            isOnline: true,
            roomId: regionalPresence.roomId,
            roomType: 'regional',
            activityId: ''
        };
    }

    if (presenceEntries.length) {
        return {
            code: 'exploring',
            label: 'Exploring nearby',
            isOnline: true,
            roomId: '',
            roomType: '',
            activityId: ''
        };
    }

    return {
        code: 'offline',
        label: 'Offline',
        isOnline: false,
        roomId: '',
        roomType: '',
        activityId: '',
        lastSeen: user && user.lastSeen ? new Date(user.lastSeen).toISOString() : ''
    };
}

function serializeFriendNotification(notification, actorUser = null) {
    const source = notification.toObject ? notification.toObject() : notification;
    const actor = actorUser
        ? (actorUser.id ? actorUser : serializePublicUser(actorUser))
        : null;

    return {
        id: String(source._id),
        type: source.type,
        title: source.title || '',
        body: source.body || '',
        roomId: source.roomId || '',
        roomType: source.roomType || '',
        activityId: source.activityId || '',
        isRead: Boolean(source.isRead),
        createdAt: source.createdAt,
        actor
    };
}

function serializeRelationshipState(relationship, currentUserId) {
    if (!relationship) return null;
    const normalizedUserId = String(currentUserId || '');
    const isRequester = String(relationship.requesterId) === normalizedUserId;
    return {
        relationshipId: String(relationship._id),
        status: relationship.status,
        direction: relationship.status === 'accepted'
            ? 'accepted'
            : (isRequester ? 'outgoing' : 'incoming')
    };
}

function sortFriendEntries(left, right) {
    const priority = {
        hosting_activity: 0,
        in_activity: 1,
        in_regional_chat: 2,
        exploring: 3,
        offline: 4
    };
    const leftPriority = priority[left.status.code] ?? 10;
    const rightPriority = priority[right.status.code] ?? 10;
    if (leftPriority !== rightPriority) return leftPriority - rightPriority;
    return String(left.user.name || left.user.username || '').localeCompare(String(right.user.name || right.user.username || ''));
}

async function buildFriendsSummaryForUser(userId) {
    const normalizedUserId = String(userId || '');
    if (!normalizedUserId) {
        return {
            friends: [],
            incomingRequests: [],
            outgoingRequests: [],
            notifications: [],
            unreadCount: 0
        };
    }

    const [relationships, notifications, unreadCount, liveActivities] = await Promise.all([
        listRelationshipsForUser(normalizedUserId),
        listFriendNotificationsByUser(normalizedUserId),
        countUnreadFriendNotifications(normalizedUserId),
        listActivities()
    ]);

    const relatedUserIds = new Set();
    relationships.forEach((relationship) => {
        relatedUserIds.add(String(relationship.requesterId));
        relatedUserIds.add(String(relationship.recipientId));
    });
    notifications.forEach((notification) => {
        if (notification.actorId) relatedUserIds.add(String(notification.actorId));
    });
    relatedUserIds.delete(normalizedUserId);

    const relatedUsers = await findUsersByIds([...relatedUserIds]);
    const userById = new Map(relatedUsers.map((user) => [String(user._id), user]));
    const socketPresenceByUserId = collectSocketPresenceByUserId();
    const hostedActivityByUserId = new Map();

    liveActivities.forEach((activity) => {
        const hostId = String(activity.hostId || '');
        if (hostId && !hostedActivityByUserId.has(hostId)) hostedActivityByUserId.set(hostId, activity);
    });

    const friends = relationships
        .filter((relationship) => relationship.status === 'accepted')
        .map((relationship) => {
            const friendId = String(relationship.requesterId) === normalizedUserId
                ? String(relationship.recipientId)
                : String(relationship.requesterId);
            const friend = userById.get(friendId);
            if (!friend) return null;
            return {
                relationshipId: String(relationship._id),
                user: serializePublicUser(friend),
                status: buildFriendPresenceStatus(
                    friend,
                    socketPresenceByUserId.get(friendId) || [],
                    hostedActivityByUserId.get(friendId) || null
                )
            };
        })
        .filter(Boolean)
        .sort(sortFriendEntries);

    const incomingRequests = relationships
        .filter((relationship) => (
            relationship.status === 'pending'
            && String(relationship.recipientId) === normalizedUserId
        ))
        .map((relationship) => {
            const requester = userById.get(String(relationship.requesterId));
            if (!requester) return null;
            return {
                relationshipId: String(relationship._id),
                user: serializePublicUser(requester),
                requestedAt: relationship.createdAt
            };
        })
        .filter(Boolean);

    const outgoingRequests = relationships
        .filter((relationship) => (
            relationship.status === 'pending'
            && String(relationship.requesterId) === normalizedUserId
        ))
        .map((relationship) => {
            const recipient = userById.get(String(relationship.recipientId));
            if (!recipient) return null;
            return {
                relationshipId: String(relationship._id),
                user: serializePublicUser(recipient),
                requestedAt: relationship.createdAt
            };
        })
        .filter(Boolean);

    return {
        friends,
        incomingRequests,
        outgoingRequests,
        notifications: notifications.map((notification) => serializeFriendNotification(
            notification,
            notification.actorId ? userById.get(String(notification.actorId)) || null : null
        )),
        unreadCount
    };
}

async function emitFriendSummaryToUser(userId) {
    const normalizedUserId = String(userId || '');
    if (!normalizedUserId) return null;
    const summary = await buildFriendsSummaryForUser(normalizedUserId);
    io.to(getUserSocketRoom(normalizedUserId)).emit('friends-summary', summary);
    return summary;
}

async function emitFriendSummaries(userIds) {
    const uniqueIds = [...new Set((userIds || []).map((userId) => String(userId || '')).filter(Boolean))];
    if (!uniqueIds.length) return;
    await Promise.all(uniqueIds.map((userId) => emitFriendSummaryToUser(userId)));
}

async function emitFriendSummariesForUserAndFriends(userId) {
    const normalizedUserId = String(userId || '');
    if (!normalizedUserId) return;
    const friendIds = await listAcceptedFriendIds(normalizedUserId);
    await emitFriendSummaries([normalizedUserId, ...friendIds]);
}

async function createAndEmitFriendNotification(userId, notificationData, actorUser) {
    const normalizedUserId = String(userId || '');
    if (!normalizedUserId) return null;

    const actorId = actorUser ? String(actorUser._id || actorUser.id || '') : '';
    const notification = await createFriendNotificationRecord({
        userId: normalizedUserId,
        actorId,
        type: notificationData.type,
        title: trimToLength(notificationData.title, 140),
        body: trimToLength(notificationData.body, 240),
        roomId: String(notificationData.roomId || ''),
        roomType: String(notificationData.roomType || ''),
        activityId: String(notificationData.activityId || ''),
        isRead: false
    });

    const unreadCount = await countUnreadFriendNotifications(normalizedUserId);

    io.to(getUserSocketRoom(normalizedUserId)).emit('friend_notification', {
        notification: serializeFriendNotification(notification, actorUser),
        unreadCount
    });

    return notification;
}

async function notifyAcceptedFriends(actorUser, notificationData) {
    const actorId = String(actorUser && (actorUser._id || actorUser.id) || '');
    if (!actorId) return;
    const friendIds = (await listAcceptedFriendIds(actorId)).filter((friendId) => String(friendId) !== actorId);
    if (!friendIds.length) return;
    await Promise.all(friendIds.map((friendId) => createAndEmitFriendNotification(friendId, notificationData, actorUser)));
}

async function sendGlobalState(target = io) {
    const activities = await listActivities();
    const serialized = activities.map(serializeActivity);

    target.emit('activities-state', serialized);
    target.emit('travelers-state', {
        activeUsers: io.engine.clientsCount,
        activeActivities: serialized.length
    });
    emitHeatmapState(target);

    return serialized;
}

async function resolveSharedHub(lat, lng, preferredName) {
    const location = { type: 'Point', coordinates: [lng, lat] };

    if (!usingMongoStore()) {
        let nearestHub = null;
        let nearestDistance = Number.POSITIVE_INFINITY;

        memoryStore.hubs.forEach((hub) => {
            const [hubLng, hubLat] = hub.location.coordinates;
            const nextDistance = distanceMeters(lat, lng, hubLat, hubLng);

            if (nextDistance <= HUB_RADIUS_METERS && nextDistance < nearestDistance) {
                nearestHub = hub;
                nearestDistance = nextDistance;
            }
        });

        if (!nearestHub) {
            nearestHub = buildMemoryDocument({
                name: buildHubName(preferredName, lat, lng),
                location
            });
            memoryStore.hubs.push(nearestHub);
        }

        return nearestHub;
    }

    let hub = await Hub.findOne({
        location: {
            $near: {
                $geometry: location,
                $maxDistance: HUB_RADIUS_METERS
            }
        }
    });

    if (!hub) {
        hub = await Hub.create({
            name: buildHubName(preferredName, lat, lng),
            location
        });
    }

    return hub;
}

async function emitRoomPresence(roomId) {
    const activeSessions = await listSessionsByRoom(roomId);

    if (activeSessions.length > 0) liveOccupancy[roomId] = activeSessions.length;
    else delete liveOccupancy[roomId];

    io.to(roomId).emit('room-users-list', activeSessions.map((session) => session.username));
    io.emit('update-vibe-counts', liveOccupancy);

    return activeSessions;
}

function collectHeatmapPoints() {
    return Array.from(io.sockets.sockets.values())
        .filter((socket) => Number.isFinite(socket.currentLat) && Number.isFinite(socket.currentLng))
        .map((socket) => ({
            lat: socket.currentLat,
            lng: socket.currentLng,
            userId: socket.currentUserId || '',
            user: socket.currentUser || ''
        }));
}

function emitHeatmapState(target = io) {
    target.emit('heatmap-state', collectHeatmapPoints());
}

async function upsertSession(socket, user, roomId, roomName, roomType) {
    const displayName = getDisplayName(user);
    const source = user.toObject ? user.toObject() : user;

    socket.currentUser = displayName;
    socket.currentUserId = String(source._id);
    socket.currentProfileId = source.profileId;
    socket.currentRoom = roomId;
    socket.currentRoomName = roomName;
    socket.currentRoomType = roomType;

    await upsertSessionRecord({
        socketId: socket.id,
        username: displayName,
        userId: String(source._id),
        profileId: source.profileId,
        placeId: roomId,
        roomName,
        roomType,
        lat: Number.isFinite(socket.currentLat) ? socket.currentLat : undefined,
        lng: Number.isFinite(socket.currentLng) ? socket.currentLng : undefined
    });
}

async function removeUserFromActivity(activityId, username, userId) {
    if (!activityId || !username) return { changed: false, deleted: false };

    const activity = await findActivityRecord(activityId);
    if (!activity) return { changed: false, deleted: true };

    const nextParticipants = (activity.participants || []).filter((participant) => participant !== username);
    const nextParticipantIds = (activity.participantIds || []).filter((participantId) => String(participantId) !== String(userId));

    if (nextParticipants.length === (activity.participants || []).length && nextParticipantIds.length === (activity.participantIds || []).length) {
        return { changed: false, deleted: false, activity };
    }

    activity.participants = nextParticipants;
    activity.participantIds = nextParticipantIds;
    await saveActivityRecord(activity);

    return { changed: true, deleted: false, activity };
}

async function closeActivityRoom(activityId, payload = {}) {
    const roomId = String(activityId || '');
    if (!roomId) return;

    const socketsInRoom = await io.in(roomId).fetchSockets();
    await Promise.all(socketsInRoom.map(async (roomSocket) => {
        const liveSocket = io.sockets.sockets.get(roomSocket.id) || roomSocket;

        liveSocket.emit('activity-ended', {
            roomId,
            roomType: 'activity',
            ...payload
        });
        liveSocket.leave(roomId);
        if (liveSocket.currentRoom === roomId && liveSocket.currentRoomType === 'activity') {
            liveSocket.currentRoom = null;
            liveSocket.currentRoomName = null;
            liveSocket.currentRoomType = null;
        }
        await deleteSessionRecord(liveSocket.id);
    }));

    await emitRoomPresence(roomId);
    if (payload.hostId) await emitFriendSummariesForUserAndFriends(String(payload.hostId));
}

async function leaveCurrentRoom(socket) {
    if (!socket.currentRoom) return null;

    const previousRoom = {
        roomId: socket.currentRoom,
        roomName: socket.currentRoomName,
        roomType: socket.currentRoomType,
        username: socket.currentUser,
        userId: socket.currentUserId
    };

    socket.leave(previousRoom.roomId);
    await deleteSessionRecord(socket.id);

    if (previousRoom.roomType === 'activity') {
        previousRoom.activityChange = await removeUserFromActivity(previousRoom.roomId, previousRoom.username, previousRoom.userId);
    }

    await emitRoomPresence(previousRoom.roomId);

    socket.currentRoom = null;
    socket.currentRoomName = null;
    socket.currentRoomType = null;

    return previousRoom;
}

async function sendRoomState(socket, details) {
    const {
        roomId,
        roomName,
        roomType,
        roomMeta,
        requestId,
        placeKey,
        activity,
        user,
        joinNotice,
        summaryNotice
    } = details;

    const alreadyInRoom = socket.currentRoom === roomId;
    let previousRoom = null;

    if (!alreadyInRoom) {
        previousRoom = await leaveCurrentRoom(socket);
        socket.join(roomId);
    }

    await upsertSession(socket, user, roomId, roomName, roomType);

    const normalizedUserId = String(user && (user._id || user.id) || '').trim();
    const [history, activeSessions, relationships] = await Promise.all([
        listMessagesByRoom(roomId),
        emitRoomPresence(roomId),
        listRelationshipsForUser(normalizedUserId)
    ]);
    const friendState = buildFriendStateFromRelationships(relationships, normalizedUserId);
    const acceptedFriendIdSet = new Set(friendState.acceptedFriendIds);
    const outgoingFriendRequestIdSet = new Set(friendState.outgoingFriendRequestIds);

    const roomParticipants = roomType === 'activity' && activity
        ? buildActivityParticipantSummaries(activity, acceptedFriendIdSet, outgoingFriendRequestIdSet, normalizedUserId)
        : activeSessions.map((session) => buildRoomParticipantSummary(
            session,
            acceptedFriendIdSet,
            outgoingFriendRequestIdSet,
            normalizedUserId
        ));

    socket.emit('room-state', {
        requestId,
        placeKey,
        roomId,
        roomName,
        roomType,
        roomMeta,
        history: history.map((message) => serializeChatMessage(message)),
        activity,
        acceptedFriendIds: friendState.acceptedFriendIds,
        outgoingFriendRequestIds: friendState.outgoingFriendRequestIds,
        participants: roomParticipants
    });

    if (!alreadyInRoom && joinNotice) {
        socket.to(roomId).emit('receive-message', {
            type: 'system',
            text: joinNotice
        });
    }

    if (summaryNotice && activeSessions.length > 1) {
        socket.emit('receive-message', {
            type: 'system',
            text: summaryNotice(activeSessions.length)
        });
    }

    if (previousRoom && previousRoom.activityChange && previousRoom.activityChange.changed) {
        await sendGlobalState();
    }

    return { history, activeSessions, joinedNewRoom: !alreadyInRoom };
}

async function getUserFromToken(token) {
    try {
        const payload = jwt.verify(token, JWT_SECRET);
        return findUserById(payload.sub);
    } catch (error) {
        return null;
    }
}

async function authMiddleware(req, res, next) {
    const authHeader = String(req.headers.authorization || '');
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : '';
    const user = await getUserFromToken(token);

    if (!user) {
        res.status(401).json({ message: 'Authentication required.' });
        return;
    }

    req.user = user;
    next();
}

function sendAuthPayload(res, user, statusCode = 200) {
    res.status(statusCode).json({
        token: createToken(user),
        user: serializeUser(user)
    });
}

app.post('/api/auth/register', async (req, res) => {
    try {
        const username = normalizeUsername(req.body && req.body.username);
        const password = String(req.body && req.body.password ? req.body.password : '');

        if (!/^[a-z0-9_.-]{3,24}$/i.test(username)) {
            res.status(400).json({ message: 'Username must be 3-24 characters and use letters, numbers, dot, dash, or underscore.' });
            return;
        }

        if (password.length < 6) {
            res.status(400).json({ message: 'Password must be at least 6 characters.' });
            return;
        }

        const existing = await findUserByUsername(username);
        if (existing) {
            res.status(409).json({ message: 'That username is already taken.' });
            return;
        }

        const passwordHash = await bcrypt.hash(password, 12);
        const user = await createUserRecord({
            auth: {
                username,
                passwordHash
            },
            profile: {
                name: '',
                birthDate: null,
                gender: '',
                bio: '',
                occupation: '',
                avatarUrl: ''
            },
            isProfileComplete: false,
            lastSeen: new Date()
        });

        sendAuthPayload(res, user, 201);
    } catch (error) {
        console.error('Register error:', error);
        res.status(500).json({ message: 'Could not create your account right now.' });
    }
});

app.post('/api/auth/login', async (req, res) => {
    try {
        const username = normalizeUsername(req.body && req.body.username);
        const password = String(req.body && req.body.password ? req.body.password : '');
        const user = await findUserByUsername(username);

        if (!user) {
            res.status(401).json({ message: 'Invalid username or password.' });
            return;
        }

        const matches = await bcrypt.compare(password, user.auth.passwordHash);
        if (!matches) {
            res.status(401).json({ message: 'Invalid username or password.' });
            return;
        }

        await touchUserRecord(user);
        sendAuthPayload(res, user);
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ message: 'Could not sign you in right now.' });
    }
});

app.get('/api/auth/me', authMiddleware, async (req, res) => {
    try {
        await touchUserRecord(req.user);
        sendAuthPayload(res, req.user);
    } catch (error) {
        console.error('Get auth user error:', error);
        res.status(500).json({ message: 'Could not load your profile right now.' });
    }
});

app.put('/api/auth/profile', authMiddleware, async (req, res) => {
    try {
        const profileResult = buildUserProfilePayload(req.body);
        if (profileResult.error) {
            res.status(400).json({ message: profileResult.error });
            return;
        }

        req.user.profile = profileResult.value;
        req.user.isProfileComplete = computeProfileComplete(profileResult.value);
        req.user.lastSeen = new Date();
        await saveUserRecord(req.user);
        await emitFriendSummariesForUserAndFriends(String(req.user._id));

        sendAuthPayload(res, req.user);
    } catch (error) {
        console.error('Profile update error:', error);
        res.status(500).json({ message: 'Could not update your profile right now.' });
    }
});

app.get('/api/friends/summary', authMiddleware, async (req, res) => {
    try {
        const summary = await buildFriendsSummaryForUser(String(req.user._id));
        res.json(summary);
    } catch (error) {
        console.error('Get friends summary error:', error);
        res.status(500).json({ message: 'Could not load your friends right now.' });
    }
});

app.get('/api/users/search', authMiddleware, async (req, res) => {
    try {
        const query = trimToLength(req.query && req.query.q, 80);
        if (query.length < 2) {
            res.json({ results: [] });
            return;
        }

        const userId = String(req.user._id);
        const [matches, relationships] = await Promise.all([
            searchUsers(query, userId),
            listRelationshipsForUser(userId)
        ]);

        const relationshipByPairKey = new Map(
            relationships.map((relationship) => [buildRelationshipPairKey(relationship.requesterId, relationship.recipientId), relationship])
        );

        res.json({
            results: matches.map((user) => {
                const relationship = relationshipByPairKey.get(buildRelationshipPairKey(userId, user._id));
                return {
                    user: serializePublicUser(user),
                    relationship: serializeRelationshipState(relationship, userId)
                };
            })
        });
    } catch (error) {
        console.error('User search error:', error);
        res.status(500).json({ message: 'Could not search travelers right now.' });
    }
});

app.post('/api/friends/requests', authMiddleware, async (req, res) => {
    try {
        const requesterId = String(req.user._id);
        const recipientId = String(req.body && req.body.targetUserId || '').trim();

        if (!recipientId) {
            res.status(400).json({ message: 'Choose a traveler to add first.' });
            return;
        }

        if (recipientId === requesterId) {
            res.status(400).json({ message: 'You cannot add yourself as a friend.' });
            return;
        }

        const recipient = await findUserById(recipientId);
        if (!recipient || !recipient.isProfileComplete) {
            res.status(404).json({ message: 'That traveler is not available to add right now.' });
            return;
        }

        const existingRelationship = await findRelationshipByPair(requesterId, recipientId);
        if (existingRelationship) {
            if (existingRelationship.status === 'accepted') {
                res.status(409).json({ message: 'You are already friends.' });
                return;
            }

            if (String(existingRelationship.requesterId) === requesterId) {
                res.status(409).json({ message: 'Friend request already sent.' });
                return;
            }

            res.status(409).json({ message: 'This traveler already sent you a request. Accept it from your drawer.' });
            return;
        }

        await createRelationshipRecord({
            pairKey: buildRelationshipPairKey(requesterId, recipientId),
            requesterId,
            recipientId,
            status: 'pending',
            acceptedAt: null
        });

        await emitFriendSummaries([requesterId, recipientId]);

        res.status(201).json({ message: 'Friend request sent.' });
    } catch (error) {
        console.error('Create friend request error:', error);
        res.status(500).json({ message: 'Could not send that friend request right now.' });
    }
});

app.post('/api/friends/requests/:relationshipId/accept', authMiddleware, async (req, res) => {
    try {
        const userId = String(req.user._id);
        const relationshipId = String(req.params.relationshipId || '').trim();
        const relationship = await findRelationshipById(relationshipId);

        if (!relationship) {
            res.status(404).json({ message: 'That friend request was not found.' });
            return;
        }

        if (relationship.status === 'accepted') {
            res.status(409).json({ message: 'You are already friends.' });
            return;
        }

        if (String(relationship.recipientId) !== userId) {
            res.status(403).json({ message: 'Only the invited traveler can accept this request.' });
            return;
        }

        relationship.status = 'accepted';
        relationship.acceptedAt = new Date();
        await saveRelationshipRecord(relationship);

        await emitFriendSummaries([String(relationship.requesterId), String(relationship.recipientId)]);

        res.json({ message: 'Friend request accepted.' });
    } catch (error) {
        console.error('Accept friend request error:', error);
        res.status(500).json({ message: 'Could not accept that friend request right now.' });
    }
});

app.post('/api/friends/notifications/read-all', authMiddleware, async (req, res) => {
    try {
        const userId = String(req.user._id);
        await markFriendNotificationsRead(userId);
        const summary = await buildFriendsSummaryForUser(userId);
        io.to(getUserSocketRoom(userId)).emit('friends-summary', summary);
        res.json({
            message: 'Notifications marked as read.',
            unreadCount: summary.unreadCount,
            notifications: summary.notifications
        });
    } catch (error) {
        console.error('Mark friend notifications read error:', error);
        res.status(500).json({ message: 'Could not update your notifications right now.' });
    }
});

app.get('/api/activities/hosting-status', authMiddleware, async (req, res) => {
    try {
        const status = await getActivityHostQuotaStatus(String(req.user._id));
        res.json(status);
    } catch (error) {
        console.error('Get hosting status error:', error);
        res.status(500).json({ message: 'Could not load your hosting availability right now.' });
    }
});

app.get('/api/activities/history', authMiddleware, async (req, res) => {
    try {
        const history = await listHostedActivityHistory(String(req.user._id));
        res.json({
            activities: history.map(serializeActivity)
        });
    } catch (error) {
        console.error('Get activity history error:', error);
        res.status(500).json({ message: 'Could not load your activity history right now.' });
    }
});

app.post('/api/activities/:activityId/end', authMiddleware, async (req, res) => {
    try {
        const activityId = String(req.params.activityId || '').trim();
        if (!activityId) {
            res.status(400).json({ message: 'Choose a valid activity to end.' });
            return;
        }

        await pruneExpiredActivities();

        const activity = await findStoredActivityRecord(activityId);
        if (!activity) {
            res.status(404).json({ message: 'That activity is not available anymore.' });
            return;
        }

        if (String(activity.hostId || '') !== String(req.user._id)) {
            res.status(403).json({ message: 'Only the host can end this activity early.' });
            return;
        }

        if (isActivityManuallyOrSoftExpired(activity) || isActivityExpired(activity)) {
            if (!isActivityManuallyOrSoftExpired(activity) && isActivityExpired(activity)) {
                await expireActivityRecord(activity, { endedAt: new Date(), endedByHost: false });
            }
            const status = await getActivityHostQuotaStatus(String(req.user._id));
            res.status(409).json({
                message: 'This activity has already ended.',
                hostingStatus: status
            });
            return;
        }

        const endedActivity = await expireActivityRecord(activity, {
            endedAt: new Date(),
            endedByHost: true
        });

        await closeActivityRoom(activityId, {
            activityId,
            hostId: String(req.user._id),
            host: getDisplayName(req.user),
            placeName: activity.placeName || '',
            goal: activity.goal || '',
            endedByHost: true
        });

        await sendGlobalState();
        await emitFriendSummariesForUserAndFriends(String(req.user._id));

        const hostingStatus = await getActivityHostQuotaStatus(String(req.user._id));
        res.json({
            message: "You're all set! One hosting slot is free again.",
            activity: serializeActivity(endedActivity),
            hostingStatus
        });
    } catch (error) {
        console.error('End activity error:', error);
        res.status(500).json({ message: 'Could not end that activity right now.' });
    }
});

io.use(async (socket, next) => {
    const token = socket.handshake && socket.handshake.auth ? socket.handshake.auth.token : '';
    const user = await getUserFromToken(token);

    if (!user) {
        next(new Error('Authentication required. Please log in again.'));
        return;
    }

    if (!user.isProfileComplete) {
        next(new Error('Please finish your profile before joining GeoChat.'));
        return;
    }

    socket.authUserId = String(user._id);
    socket.currentUser = getDisplayName(user);
    socket.currentUserId = String(user._id);
    socket.currentProfileId = user.profileId;
    socket.currentRoom = null;
    socket.currentRoomName = null;
    socket.currentRoomType = null;

    await touchUserRecord(user);
    next();
});

async function getSocketUser(socket) {
    const user = await findUserById(socket.authUserId);
    if (!user) {
        socket.emit('room-error', { message: 'Your session expired. Please log in again.' });
        return null;
    }

    if (!user.isProfileComplete) {
        socket.emit('room-error', { message: 'Please finish your profile before joining activities.' });
        return null;
    }

    socket.currentUser = getDisplayName(user);
    socket.currentUserId = String(user._id);
    socket.currentProfileId = user.profileId;
    await touchUserRecord(user);
    return user;
}

io.on('connection', (socket) => {
    console.log('New traveler connected:', socket.id);
    socket.roomSequence = 0;
    socket.currentLat = null;
    socket.currentLng = null;
    socket.join(getUserSocketRoom(socket.currentUserId));

    sendGlobalState(socket).catch((error) => {
        console.error('Global state bootstrap error:', error);
    });

    sendGlobalState().catch((error) => {
        console.error('Global state broadcast error:', error);
    });

    emitFriendSummariesForUserAndFriends(socket.currentUserId).catch((error) => {
        console.error('Friend summary bootstrap error:', error);
    });

    socket.on('request-global-state', async () => {
        try {
            const user = await getSocketUser(socket);
            if (!user) return;
            await sendGlobalState(socket);
        } catch (error) {
            console.error('Request global state error:', error);
        }
    });

    socket.on('update-user-location', async (data = {}) => {
        const user = await getSocketUser(socket);
        if (!user) return;

        const lat = Number(data.lat);
        const lng = Number(data.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

        socket.currentLat = lat;
        socket.currentLng = lng;

        if (socket.currentRoom) {
            await upsertSessionRecord({
                socketId: socket.id,
                username: socket.currentUser,
                userId: socket.currentUserId,
                profileId: socket.currentProfileId,
                placeId: socket.currentRoom,
                roomName: socket.currentRoomName,
                roomType: socket.currentRoomType,
                lat,
                lng
            });
        }

        emitHeatmapState();
    });

    socket.on('join-room', async (data = {}) => {
        const user = await getSocketUser(socket);
        if (!user) return;

        const lat = Number(data.lat);
        const lng = Number(data.lng);
        const preferredRoomName = data.roomName;
        const requestId = data.requestId;
        const placeKey = data.placeKey;
        const displayName = getDisplayName(user);

        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
            socket.emit('room-error', { message: 'Could not open the 5 km chat for this location.' });
            return;
        }

        socket.roomSequence += 1;
        const sequence = socket.roomSequence;

        try {
            const hub = await resolveSharedHub(lat, lng, preferredRoomName);

            if (sequence !== socket.roomSequence) return;

            const roomState = await sendRoomState(socket, {
                roomId: hub._id.toString(),
                roomName: hub.name,
                roomType: 'regional',
                roomMeta: `${HUB_RADIUS_METERS / 1000} km Regional Hub`,
                requestId,
                placeKey,
                user,
                joinNotice: `${displayName} joined the chat near ${hub.name}.`,
                summaryNotice: (count) => `There are ${count - 1} other traveler(s) in this 5 km zone. Say hi!`
            });

            if (roomState.joinedNewRoom) {
                await notifyAcceptedFriends(user, {
                    type: 'friend_joined_room',
                    title: `${displayName} joined a room`,
                    body: `${displayName} joined the 5 km chat near ${hub.name}.`,
                    roomId: String(hub._id),
                    roomType: 'regional',
                    activityId: ''
                });
                await emitFriendSummariesForUserAndFriends(String(user._id));
            }
        } catch (error) {
            console.error('Join regional room error:', error);
            socket.emit('room-error', { message: 'Could not open the 5 km chat right now.' });
        }
    });

    socket.on('create-activity', async (data = {}) => {
        const user = await getSocketUser(socket);
        if (!user) return;

        const lat = Number(data.lat);
        const lng = Number(data.lng);
        const goal = typeof data.goal === 'string' ? data.goal.trim() : '';
        const theme = getValidTheme(data.theme, CREATABLE_ACTIVITY_THEMES);
        const scheduledFor = normalizeActivityDateKey(data.scheduledFor);
        const ageRange = buildActivityAgeRange(data.ageMin, data.ageMax);
        const placeName = typeof data.placeName === 'string' && data.placeName.trim() ? data.placeName.trim() : 'Dropped Pin';
        const placeKey = typeof data.placeKey === 'string' ? data.placeKey : '';
        const vicinity = typeof data.vicinity === 'string' ? data.vicinity.trim() : '';
        const requestId = data.requestId;
        const displayName = getDisplayName(user);

        if (!Number.isFinite(lat) || !Number.isFinite(lng) || !goal) {
            socket.emit('room-error', { message: 'Add a goal and location before hosting an activity.' });
            return;
        }
        if (!theme) {
            socket.emit('room-error', { message: 'Choose a valid activity theme before hosting.' });
            return;
        }
        if (!scheduledFor) {
            socket.emit('room-error', { message: 'Choose a valid day before hosting this activity.' });
            return;
        }

        const userId = String(user._id);
        if (pendingActivityCreations.has(userId)) {
            socket.emit('room-error', { message: 'We are already creating one of your activities. Please wait a moment.' });
            return;
        }

        socket.roomSequence += 1;
        const sequence = socket.roomSequence;

        pendingActivityCreations.add(userId);
        try {
            const hostingStatus = await getActivityHostQuotaStatus(userId);
            if (hostingStatus.isLimited) {
                socket.emit('room-error', {
                    message: `You can host up to ${ACTIVITY_HOST_LIMIT} activities every 24 hours. Available again in ${formatDurationCompact(hostingStatus.retryAfterMs)}.`
                });
                return;
            }

            const activity = await createActivityRecord({
                host: displayName,
                hostId: userId,
                goal: goal.slice(0, 180),
                theme,
                scheduledFor,
                ageMin: ageRange.min,
                ageMax: ageRange.max,
                placeName: placeName.slice(0, 120),
                placeKey,
                vicinity: vicinity.slice(0, 180),
                participants: [displayName],
                participantIds: [String(user._id)],
                location: { type: 'Point', coordinates: [lng, lat] }
            });

            if (sequence !== socket.roomSequence) return;

            const serializedActivity = serializeActivity(activity);

            await sendRoomState(socket, {
                roomId: serializedActivity.id,
                roomName: serializedActivity.goal,
                roomType: 'activity',
                roomMeta: `${getThemeLabel(serializedActivity.theme)} hosted by ${serializedActivity.host} at ${serializedActivity.placeName}`,
                requestId,
                placeKey: serializedActivity.placeKey,
                activity: serializedActivity,
                user,
                joinNotice: `${displayName} started a new activity: ${serializedActivity.goal}.`,
                summaryNotice: (count) => `There are ${count - 1} other traveler(s) in this activity.`
            });

            await sendGlobalState();
            await notifyAcceptedFriends(user, {
                type: 'friend_hosted_activity',
                title: `${displayName} hosted a new activity`,
                body: `${displayName} is hosting ${getThemeLabel(serializedActivity.theme)} at ${serializedActivity.placeName || 'a nearby spot'}.`,
                roomId: serializedActivity.id,
                roomType: 'activity',
                activityId: serializedActivity.id
            });
            await emitFriendSummariesForUserAndFriends(userId);
        } catch (error) {
            console.error('Create activity error:', error);
            socket.emit('room-error', { message: 'Could not create that activity right now.' });
        } finally {
            pendingActivityCreations.delete(userId);
        }
    });

    socket.on('join-activity', async (data = {}) => {
        const user = await getSocketUser(socket);
        if (!user) return;

        const activityId = typeof data.activityId === 'string' ? data.activityId : '';
        const requestId = data.requestId;
        const displayName = getDisplayName(user);
        const userId = String(user._id);

        if (!activityId) {
            socket.emit('room-error', { message: 'That activity is not available anymore.' });
            return;
        }

        socket.roomSequence += 1;
        const sequence = socket.roomSequence;

        try {
            let activity = await findActivityRecord(activityId);

            if (!activity) {
                socket.emit('room-error', { message: 'That activity is no longer active.' });
                await sendGlobalState();
                return;
            }

            if (!(activity.participantIds || []).includes(userId)) {
                activity.participants.push(displayName);
                activity.participantIds = [...(activity.participantIds || []), userId];
                await saveActivityRecord(activity);
                activity = await findActivityRecord(activityId);
            }

            if (sequence !== socket.roomSequence) return;

            const serializedActivity = serializeActivity(activity);

            const roomState = await sendRoomState(socket, {
                roomId: serializedActivity.id,
                roomName: serializedActivity.goal,
                roomType: 'activity',
                roomMeta: `${getThemeLabel(serializedActivity.theme)} hosted by ${serializedActivity.host} at ${serializedActivity.placeName}`,
                requestId,
                placeKey: serializedActivity.placeKey,
                activity: serializedActivity,
                user,
                joinNotice: `${displayName} joined ${serializedActivity.host}'s activity.`,
                summaryNotice: (count) => `There are ${count - 1} other traveler(s) in this activity.`
            });

            await sendGlobalState();
            if (roomState.joinedNewRoom) {
                await notifyAcceptedFriends(user, {
                    type: 'friend_joined_room',
                    title: `${displayName} joined a room`,
                    body: `${displayName} joined the activity "${serializedActivity.goal}".`,
                    roomId: serializedActivity.id,
                    roomType: 'activity',
                    activityId: serializedActivity.id
                });
                await emitFriendSummariesForUserAndFriends(userId);
            }
        } catch (error) {
            console.error('Join activity error:', error);
            socket.emit('room-error', { message: 'Could not join that activity right now.' });
        }
    });

    socket.on('leave-room', async () => {
        try {
            const previousRoom = await leaveCurrentRoom(socket);
            if (!previousRoom) return;

            if (previousRoom.roomType === 'activity' && previousRoom.activityChange && previousRoom.activityChange.changed) {
                await sendGlobalState();
            }

            await emitFriendSummariesForUserAndFriends(previousRoom.userId || socket.currentUserId);

            socket.emit('room-left', {
                roomId: previousRoom.roomId,
                roomName: previousRoom.roomName,
                roomType: previousRoom.roomType
            });
        } catch (error) {
            console.error('Leave room error:', error);
            socket.emit('room-error', { message: 'Could not leave the room right now.' });
        }
    });

    socket.on('leave-activity', async (data = {}) => {
        const activityId = typeof data.activityId === 'string' ? data.activityId : socket.currentRoom;

        try {
            if (!activityId) return;

            if (socket.currentRoom === activityId && socket.currentRoomType === 'activity') {
                const previousRoom = await leaveCurrentRoom(socket);

                if (previousRoom && previousRoom.activityChange && previousRoom.activityChange.changed) {
                    await sendGlobalState();
                }

                socket.emit('room-left', {
                    roomId: activityId,
                    roomType: 'activity'
                });
                await emitFriendSummariesForUserAndFriends(previousRoom ? previousRoom.userId : socket.currentUserId);
                return;
            }

            const activityChange = await removeUserFromActivity(activityId, socket.currentUser, socket.currentUserId);
            if (activityChange.changed) await sendGlobalState();
            await emitFriendSummariesForUserAndFriends(socket.currentUserId);

            socket.emit('room-left', {
                roomId: activityId,
                roomType: 'activity'
            });
        } catch (error) {
            console.error('Leave activity error:', error);
            socket.emit('room-error', { message: 'Could not leave the activity right now.' });
        }
    });

    socket.on('send-message', async (data = {}) => {
        const user = await getSocketUser(socket);
        if (!user) return;

        const room = typeof data.room === 'string' ? data.room : '';
        const roomName = typeof data.roomName === 'string' ? data.roomName : '';
        const text = typeof data.text === 'string' ? data.text.trim() : '';
        const time = typeof data.time === 'string' ? data.time : '';

        if (!room || !text) return;

        const message = {
            room,
            roomName,
            user: getDisplayName(user),
            userId: String(user._id),
            text: text.slice(0, 400),
            time
        };

        try {
            await createMessageRecord(message);
            io.to(room).emit('receive-message', message);
        } catch (error) {
            console.error('Send message error:', error);
        }
    });

    socket.on('disconnect', async () => {
        console.log('Traveler left:', socket.id);

        try {
            const previousRoom = await leaveCurrentRoom(socket);
            if (previousRoom && previousRoom.roomType === 'activity' && previousRoom.activityChange && previousRoom.activityChange.changed) {
                await sendGlobalState();
            } else {
                await sendGlobalState();
            }
            await emitFriendSummariesForUserAndFriends(socket.currentUserId);
        } catch (error) {
            console.error('Disconnect cleanup error:', error);
        }
    });
});

async function initializeStorage() {
    try {
        await mongoose.connect('mongodb://127.0.0.1:27017/Geochat_DB', {
            serverSelectionTimeoutMS: 2500
        });
        storageMode = 'mongo';
        console.log('Connected to Geochat_DB');
    } catch (error) {
        storageMode = 'memory';
        console.warn('MongoDB unavailable, using in-memory storage for GeoChat.');
        console.warn(error.message);
    }
}

initializeStorage().finally(() => {
    server.listen(PORT, () => {
        console.log(`
    GeoChat Server Running
    Local: http://localhost:${PORT}
    Store: ${storageMode === 'mongo' ? 'Geochat_DB' : 'In-memory fallback'}
    `);
    });
});

setInterval(async () => {
    try {
        const removedIds = await pruneExpiredActivities();
        if (removedIds.length) await sendGlobalState();
    } catch (error) {
        console.error('Activity cleanup error:', error);
    }
}, ACTIVITY_CLEANUP_INTERVAL_MS);
