db.streams.aggregate([
  {
    $lookup: {
      from: "songs",
      localField: "songId",
      foreignField: "_id",
      as: "song"
    }
  },
  { $unwind: "$song" },
  {
    $match: { "song.genre": "Reggaeton" }
  },
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
      output: {
        count: { $sum: 1 }
      }
    }
  }
]);
