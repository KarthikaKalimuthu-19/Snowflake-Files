use BookStoreManagement
switched to db BookStoreManagement
db["books"].find()

// 1. books

db.books.insertMany([
  { book_id: 101, title: "The AI Revolution", author: "Ray Kurzweil", genre: "Technology", price: 799, stock: 20 },
  { book_id: 102, title: "Lost in the Woods", author: "John Green", genre: "Fiction", price: 450, stock: 10 },
  { book_id: 103, title: "Quantum Physics 101", author: "Dr. Neil Tyson", genre: "Science", price: 950, stock: 5 },
  { book_id: 104, title: "Cooking Made Easy", author: "Jamie Oliver", genre: "Cooking", price: 350, stock: 15 },
  { book_id: 105, title: "Ancient Civilizations", author: "Mary Beard", genre: "History", price: 620, stock: 8 }
]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('687f2d826c6e4c33b5f549c8'),
    '1': ObjectId('687f2d826c6e4c33b5f549c9'),
    '2': ObjectId('687f2d826c6e4c33b5f549ca'),
    '3': ObjectId('687f2d826c6e4c33b5f549cb'),
    '4': ObjectId('687f2d826c6e4c33b5f549cc')
  }
}

// 2. customers

db.customers.insertMany([
  { customer_id: 201, name: "Ananya Rao", email: "ananya@example.com", city: "Hyderabad" },
  { customer_id: 202, name: "Rahul Mehra", email: "rahul@example.com", city: "Delhi" },
  { customer_id: 203, name: "Sneha Kapoor", email: "sneha@example.com", city: "Hyderabad" },
  { customer_id: 204, name: "Vikram Joshi", email: "vikram@example.com", city: "Mumbai" },
  { customer_id: 205, name: "Ishita Sen", email: "ishita@example.com", city: "Kolkata" }
]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('687f2dc56c6e4c33b5f549cd'),
    '1': ObjectId('687f2dc56c6e4c33b5f549ce'),
    '2': ObjectId('687f2dc56c6e4c33b5f549cf'),
    '3': ObjectId('687f2dc56c6e4c33b5f549d0'),
    '4': ObjectId('687f2dc56c6e4c33b5f549d1')
  }
}

// 3. orders

db.orders.insertMany([
  { order_id: 301, customer_id: 201, book_id: 101, order_date: ISODate("2023-03-01"), quantity: 1 },
  { order_id: 302, customer_id: 202, book_id: 102, order_date: ISODate("2022-12-15"), quantity: 2 },
  { order_id: 303, customer_id: 203, book_id: 103, order_date: ISODate("2024-01-10"), quantity: 1 },
  { order_id: 304, customer_id: 204, book_id: 104, order_date: ISODate("2023-06-05"), quantity: 3 },
  { order_id: 305, customer_id: 201, book_id: 105, order_date: ISODate("2023-02-20"), quantity: 2 },
  { order_id: 306, customer_id: 205, book_id: 101, order_date: ISODate("2024-03-03"), quantity: 1 },
  { order_id: 307, customer_id: 203, book_id: 105, order_date: ISODate("2023-05-22"), quantity: 1 }
]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('687f2dec6c6e4c33b5f549d2'),
    '1': ObjectId('687f2dec6c6e4c33b5f549d3'),
    '2': ObjectId('687f2dec6c6e4c33b5f549d4'),
    '3': ObjectId('687f2dec6c6e4c33b5f549d5'),
    '4': ObjectId('687f2dec6c6e4c33b5f549d6'),
    '5': ObjectId('687f2dec6c6e4c33b5f549d7'),
    '6': ObjectId('687f2dec6c6e4c33b5f549d8')
  }
}

// Basic Queries:

// 1. List all books priced above 500.
db.books.find({price: {$gt: 500}});

// 2. Show all customers from ‘Hyderabad’.
db.customers.find({city: "Hyderabad"});

// 3. Find all orders placed after January 1, 2023.
db.orders.find({order_date: { $gt: ISODate("2023-01-01")}});

// Joins via $lookup :

// 4. Display order details with customer name and book title.
db.orders.aggregate([
  {
    $lookup: {
      from: "customers",
      localField: "customer_id",
      foreignField: "customer_id",
      as: "customer"
    }
  },
  { $unwind: "$customer" },
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book"
    }
  },
  { $unwind: "$book" },
  {
    $project: {
      _id: 0,
      order_id: 1,
      customer_name: "$customer.name",
      book_title: "$book.title",
      order_date: 1,
      quantity: 1
    }
  }
]);

// 5. Show total quantity ordered for each book.
db.orders.aggregate([
  {
    $group: {
      _id: "$book_id",
      total_quantity: { $sum: "$quantity" }
    }
  },
  {
    $lookup: {
      from: "books",
      localField: "_id",
      foreignField: "book_id",
      as: "book"
    }
  },
  { $unwind: "$book" },
  {
    $project: {
      book_title: "$book.title",
      total_quantity: 1
    }
  }
]);

// 6. Show the total number of orders placed by each customer.
db.orders.aggregate([
  {
    $group: {
      _id: "$customer_id",
      total_orders: { $sum: 1 }
    }
  },
  {
    $lookup: {
      from: "customers",
      localField: "_id",
      foreignField: "customer_id",
      as: "customer"
    }
  },
  { $unwind: "$customer" },
  {
    $project: {
      customer_name: "$customer.name",
      total_orders: 1
    }
  }
]);

// Aggregation Queries:

// 7. Calculate total revenue generated per book.
db.orders.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book"
    }
  },
  { $unwind: "$book" },
  {
    $project: {
      book_id: 1,
      revenue: { $multiply: ["$quantity", "$book.price"] },
      book_title: "$book.title"
    }
  },
  {
    $group: {
      _id: "$book_id",
      book_title: { $first: "$book_title" },
      total_revenue: { $sum: "$revenue" }
    }
  }
]);

// 8. Find the book with the highest total revenue.
db.orders.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book"
    }
  },
  { $unwind: "$book" },
  {
    $project: {
      revenue: { $multiply: ["$quantity", "$book.price"] },
      book_title: "$book.title"
    }
  },
  {
    $group: {
      _id: "$book_title",
      total_revenue: { $sum: "$revenue" }
    }
  },
  { $sort: { total_revenue: -1 } },
  { $limit: 1 }
]);

// 9. List genres and total books sold in each genre.
db.orders.aggregate([
  {
    $lookup: {
      from: "books",
      localField: "book_id",
      foreignField: "book_id",
      as: "book"
    }
  },
  { $unwind: "$book" },
  {
    $group: {
      _id: "$book.genre",
      total_sold: { $sum: "$quantity" }
    }
  }
]);

// 10. Show customers who ordered more than 2 different books.
db.orders.aggregate([
  {
    $group: {
      _id: { customer_id: "$customer_id", book_id: "$book_id" }
    }
  },
  {
    $group: {
      _id: "$_id.customer_id",
      distinct_books: { $sum: 1 }
    }
  },
  {
    $match: {
      distinct_books: { $gt: 2 }
    }
  },
  {
    $lookup: {
      from: "customers",
      localField: "_id",
      foreignField: "customer_id",
      as: "customer"
    }
  },
  { $unwind: "$customer" },
  {
    $project: {
      customer_name: "$customer.name",
      distinct_books: 1
    }
  }
]);


