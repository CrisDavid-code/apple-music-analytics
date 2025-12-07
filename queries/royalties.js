const lastMonthStart = new Date();
lastMonthStart.setMonth(lastMonthStart.getMonth() - 1);

db.streams.aggregate([
  {
    $match: {
      playedAt: { $gte: lastMonthStart }
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
      _id: 0,
      artistId: "$artist._id",
      artistName: "$artist.name",
      totalSeconds: 1
    }
  },
  { $sort: { totalSeconds: -1 } }
]);
