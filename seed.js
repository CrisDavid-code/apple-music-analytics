// seed.js
const { MongoClient, ObjectId } = require("mongodb");
const uri = "mongodb://root:example@localhost:27017/?authSource=admin";
const dbName = "apple_music";

const countries = ["Guatemala", "USA", "Mexico", "Colombia", "Spain"];
const genres = ["Reggaeton", "Pop", "Rock", "Hip-Hop", "Electronic"];

function randomFrom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomDateWithinLastMonths(months = 2) {
  const now = new Date();
  const past = new Date();
  past.setMonth(now.getMonth() - months);
  return new Date(past.getTime() + Math.random() * (now.getTime() - past.getTime()));
}

function randomAge() {
  return 15 + Math.floor(Math.random() * 40);
}

async function run() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);

  const usersCol = db.collection("users");
  const artistsCol = db.collection("artists");
  const songsCol = db.collection("songs");
  const streamsCol = db.collection("streams");

  // Limpieza
  await Promise.all([
    usersCol.deleteMany({}),
    artistsCol.deleteMany({}),
    songsCol.deleteMany({}),
    streamsCol.deleteMany({})
  ]);

  // 1) USUARIOS

  const users = [];
  for (let i = 0; i < 50; i++) {
    users.push({
      name: `User ${i + 1}`,
      email: `user${i + 1}@example.com`,
      country: randomFrom(countries),
      age: randomAge(),
      subscriptionType: Math.random() > 0.3 ? "premium" : "free"
    });
  }
  const usersResult = await usersCol.insertMany(users);


  // 2) ARTISTAS

  const artistNames = ["Bad Bunny", "Karol G", "Feid", "Taylor Swift", "Ed Sheeran"];
  const artists = artistNames.map(name => ({ name, country: "N/A" }));
  const artistsResult = await artistsCol.insertMany(artists);

  // 3) CANCIONES

  const songs = [];
  Object.values(artistsResult.insertedIds).forEach((artistId, index) => {
    for (let i = 0; i < 20; i++) {
      songs.push({
        title: `Song ${index + 1}-${i + 1}`,
        artistId,
        artistName: artistNames[index],
        genre: randomFrom(genres),
        duration: 120 + Math.floor(Math.random() * 180)
      });
    }
  });
  const songsResult = await songsCol.insertMany(songs);

  // 4) STREAMS

  const userIds = Object.values(usersResult.insertedIds);
  const songDocs = await songsCol.find().toArray();
  const streams = [];

  for (let i = 0; i < 2000; i++) {
    const user = randomFrom(userIds);
    const song = randomFrom(songDocs);
    streams.push({
      userId: user,
      songId: song._id,
      artistId: song.artistId,
      artistName: song.artistName,
      userCountry: randomFrom(countries),
      playedAt: randomDateWithinLastMonths(2),
      duration: song.duration
    });
  }
  await streamsCol.insertMany(streams);

  // 5) FORZAR 5 ZOMBIES PREMIUM

  const premiumUsers = await usersCol.find({ subscriptionType: "premium" }).toArray();
  const zombies = premiumUsers.slice(0, 5);

  await streamsCol.deleteMany({
    userId: { $in: zombies.map(u => u._id) }
  });

  console.log("🧟‍♂️ Se generaron 5 usuarios premium sin actividad (Zombies)");

  console.log("Seeding completo");
  await client.close();
}

run().catch(console.error);
