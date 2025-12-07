db.streams.aggregate([
  { $match: { artistName: "Bad Bunny" } },
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
]);
