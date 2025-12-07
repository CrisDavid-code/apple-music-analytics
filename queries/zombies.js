const thirtyDaysAgo = new Date();
thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

db.users.aggregate([
  {
    $match: { plan: "Premium" }
  },
  {
    $lookup: {
      from: "streams",
      let: { userId: "$_id" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$userId", "$$userId"] },
                { $gte: ["$playedAt", thirtyDaysAgo] }
              ]
            }
          }
        }
      ],
      as: "recentStreams"
    }
  },
  {
    $match: {
      recentStreams: { $size: 0 }
    }
  },
  {
    $project: {
      _id: 1,
      name: 1,
      email: 1,
      country: 1
    }
  }
]);
