const sevenDaysAgo = new Date();
sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

db.streams.aggregate([
  {
    $match: {
      userCountry: "GT",
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
      _id: 0,
      songId: "$song._id",
      title: "$song.title",
      artistName: "$song.artistName",
      plays: 1
    }
  }
]);
