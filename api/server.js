const express = require("express");
const { MongoClient, ObjectId } = require("mongodb");

const app = express();
const PORT = 3000;

//  CONEXIÓN A MONGO
const uri = "mongodb://root:example@localhost:27017/?authSource=admin";
const client = new MongoClient(uri);

async function connectDB() {
  if (!client.topology || !client.topology.isConnected()) {
    await client.connect();
  }
  return client.db("apple_music");
}

//  ENDPOINT 1 — ROYALTIES
app.get("/api/royalties", async (req, res) => {
  try {
    const db = await connectDB();

    const result = await db.collection("streams").aggregate([
      {
        $match: {
          playedAt: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) }
        }
      },
      {
        $group: {
          _id: "$artistId",
          totalSeconds: { $sum: "$duration" }
        }
      },
      {
        $lookup: {
          from: "artists",
          localField: "_id",
          foreignField: "_id",
          as: "artist"
        }
      },
      { $unwind: "$artist" },
      {
        $project: {
          artistId: "$artist._id",
          artistName: "$artist.name",
          totalSeconds: 1,
          _id: 0
        }
      },
      { $sort: { totalSeconds: -1 } }
    ]).toArray();

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

//  ENDPOINT 2 — TOP 10 SONGS

app.get("/api/top-songs", async (req, res) => {
  try {
    const db = await connectDB();

    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

    const result = await db.collection("streams").aggregate([
      { 
        $match: { 
          userCountry: "Guatemala",
          playedAt: { $gte: sevenDaysAgo }
        } 
      },
      {
        $group: {
          _id: "$songId",
          plays: { $sum: 1 }
        }
      },
      { $sort: { plays: -1 } },
      { $limit: 10 },
      {
        $lookup: {
          from: "songs",
          localField: "_id",
          foreignField: "_id",
          as: "song"
        }
      },
      { $unwind: "$song" },
      {
        $project: {
          songId: "$song._id",
          title: "$song.title",
          artistName: "$song.artistName",
          plays: 1,
          _id: 0
        }
      }
    ]).toArray();

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

//  ENDPOINT 3 — ZOMBIES

app.get("/api/zombies", async (req, res) => {
  try {
    const db = await connectDB();

    const last30Days = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

    const result = await db.collection("users").aggregate([
      { 
        $match: { subscriptionType: "premium" } 
      },
      {
        $lookup: {
          from: "streams",
          localField: "_id",
          foreignField: "userId",
          as: "streams"
        }
      },
      {
        $addFields: {
          recentActivity: {
            $filter: {
              input: "$streams",
              cond: {
                $gte: ["$$this.playedAt", last30Days]
              }
            }
          }
        }
      },
      {
        $match: { recentActivity: { $size: 0 } }
      },
      {
        $project: {
          _id: 1,
          name: 1,
          email: 1,
          country: 1
        }
      }
    ]).toArray();

    res.json(result);

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

//  ENDPOINT 4 — DEMOGRAPHICS
app.get("/api/demographics", async (req, res) => {
  try {
    const db = await connectDB();

    const result = await db.collection("streams").aggregate([
      {
        $lookup: {
          from: "songs",
          localField: "songId",
          foreignField: "_id",
          as: "song"
        }
      },
      { $unwind: "$song" },
      { $match: { "song.genre": "Reggaeton" } },
      {
        $lookup: {
          from: "users",
          localField: "userId",
          foreignField: "_id",
          as: "user"
        }
      },
      { $unwind: "$user" },
      {
        $bucket: {
          groupBy: "$user.age",
          boundaries: [15, 21, 31, 41, 51, 100],
          default: "other",
          output: { listeners: { $sum: 1 } }
        }
      }
    ]).toArray();

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

//  ENDPOINT 5 — HEAVY USERS

app.get("/api/heavy-users", async (req, res) => {
  try {
    const db = await connectDB();

    const artist = req.query.artist || "Bad Bunny";

    const result = await db.collection("streams").aggregate([
      { $match: { artistName: artist } },
      {
        $group: {
          _id: "$userId",
          songsPlayed: { $addToSet: "$songId" }
        }
      },
      {
        $project: {
          userId: "$_id",
          distinctSongs: { $size: "$songsPlayed" },
          _id: 0
        }
      },
      { $sort: { distinctSongs: -1 } },
      { $limit: 5 },
      {
        $lookup: {
          from: "users",
          localField: "userId",
          foreignField: "_id",
          as: "user"
        }
      },
      { $unwind: "$user" },
      {
        $project: {
          userId: 1,
          name: "$user.name",
          distinctSongs: 1
        }
      }
    ]).toArray();

    res.json({
      artist: artist,
      heavyUsers: result
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
