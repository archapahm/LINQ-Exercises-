<Query Kind="Statements">
  <Connection>
    <ID>c6f4e718-6c32-4cec-a916-5a59a553cf40</ID>
    <NamingServiceVersion>2</NamingServiceVersion>
    <Persist>true</Persist>
    <Server>Com_puter</Server>
    <AllowDateOnlyTimeOnly>true</AllowDateOnlyTimeOnly>
    <Database>GroceryListJan2018</Database>
    <DriverData>
      <LegacyMFA>false</LegacyMFA>
    </DriverData>
  </Connection>
</Query>

/*Query 1
Create a product list which indicates what products are purchased by our customers and how many times 
that product was purchased. Order the list by most popular product then by alphabetic description.*/

OrderLists
	.GroupBy (group => new {group.Product.Description/*, group.Product.UnitSize*/})
	//.Where (c => c.Key.QtyOrdered > 1)
	.Select (list => new {
		Product = list.Key.Description,
		//UnitSize = list.Key.UnitSize,
		TimesPurchased = list.Count(),
		//MoreThan1 = list.Key.QtyOrdered.Count() //Quantas VEZES o produto X foi comprado em quantidade maior do que 1?
	})
	.OrderByDescending (order => order.TimesPurchased)
	.ThenBy (order => order.Product)
	//.Where(order => order.TimesPurchased > 20)
	.Dump();
	
//Right answer
OrderLists
	.GroupBy (g => new {g.Product.Description, g.ProductID})
	.Select (l => new {
		Product = l.Key.Description/*Products
					.Where (w => w.ProductID == l.Key.ProductID)
					.GroupBy (g => g.Description)
					.Select(p => p.Key),*/,
		TimesPurchased = l.Count()
		})
		.OrderByDescending(o => o.TimesPurchased)
		.ThenBy (t => t.Product)
	.Dump();

/*Albums
	.GroupBy(a => new { a.ReleaseLabel, a.ReleaseYear })//  Creating anonymous key set 
	.Where(albumGroup => albumGroup.Count() >= 2)
	.OrderBy(albumGroup => albumGroup.Key.ReleaseLabel)
	.Select(album => new
	{
		Label = album.Key.ReleaseLabel,
		Year = album.Key.ReleaseYear,
		NumberOfAlbums = album.Count(),
		AlbumsGroupItem = album    //  smaller detail (mini collection) 
							.Select(albumInstance => new
							{
								Title = albumInstance.Title,
								Year = albumInstance.ReleaseYear,
								NumberOfTracks = albumInstance.Tracks.Count()//  Broken 
							})
	})*/
	
	
/*OrderLists
	.OrderByDescending (o => o.QtyOrdered)
	.ThenBy (o => o.Product.Description)
	.GroupBy (l => new {l.Product.Description, l.OrderID})
	.Select (gb => new {
	
		Product = gb.Key.Description,
		TimesPurchased = gb.Key.OrderID
		
	}).Dump();*/
	


/*OrderLists
	.Select (x => x.QtyOrdered).Sum().Dump();
	
OrderLists
	.Select (x => new {
	
		Product = x.Product.Description,
		TimesPurchased = x.OrderID
		
	}).Count().Dump();
	


OrderLists
	.Select (x => new {
		Product = x.Product.Description,
		TimesPurchased = x.OrderID
	}
	).OrderByDescending (x => x.TimesPurchased)
	.ThenBy (x => x.Product)
	.Dump();
	
OrderLists
	//.Where (x => x.Product.Description == "7Up")
	.Select (x => new {
		TimesPurchased = x.OrderID,
		Product = x.Product.Description
	}).Count()
	//.OrderBy (x => x.Product)
	.Dump();
	
Products
	.Select (x => x).Dump();
	
Categories
  .OrderBy(x => x.CategoryName)
  .Select(x => new
  {
  	CategoryName = x.CategoryName,
  	Details = Products
  				.Where(p => p.CategoryID == x.CategoryID)
  				.OrderBy(p => p.ProductName)
  				.Select(p => new
  				{
  					ProductName = p.ProductName,
  					TotalSold = p.OrderDetails.Sum(x => (int?)x.Quantity)
  				})
  })
  .Dump();*/
	
/*Query 2
We want a mailing list for a Valued Customers flyer that is being sent out. 
List the customer addresses for customers who have shopped at each store. 
List by the store. Include the store location as well as the customer's address. 
Do NOT include the customer name in the results.*/


/*Stores
	//.Where (o => o.Orders.Count() >= 1)
	.GroupBy (g => new {g.Location, g.StoreID})
	.Select (l => new {
			Location = l.Key.Location, 
			Clients = Orders
						//Where customers orders is greater or equal to 1
						//.Where (o => o.Orders.Count() >= 1) //Talvez esteja errado
						//.Where (o => o.CustomerID == o.Customer.CustomerID)
						.Where (w => w.StoreID == l.Key.StoreID)
						.Select (c => new {
							Address = c.Customer.Address,
							City = c.Customer.City,
							Province = c.Customer.Province
						})
			
	}).Dump();*/
	
Stores
	.GroupBy (g => new {g.Location, g.StoreID})
	.Select (l => new {
			Location = l.Key.Location,
			Clients = Orders
						.GroupBy(gb => new {gb.StoreID, gb.Customer.Address, gb.Customer.City, gb.Customer.Province})
						//.Where (w => w.Key.StoreID == l.Key.StoreID)
						.Select (s => new {
							Address = s.Key.Address,
							City = s.Key.City,
							Province = s.Key.Province
						})//.Distinct()
	})
	.OrderBy (o => o.Location)
	.Dump();

/*Query 3
Create a Daily Sales per Store request for a specified month.
Order stores by city by location. For Sales, show order date, number of orders, total sales without GST tax and total GST tax.*/

Stores
	//.GroupBy (g => new {g.City, g.Location, g.StoreID})
	.Select (s => new {
			City = s.City,
			Location = s.Location,
			Sales = Orders
					.GroupBy (gb => new {gb.OrderDate, gb.StoreID}) //gb.SubTotal, gb.GST/*gb.OrderLists, gb.SubTotal, gb.GST*/})
					.Where (o => o.Key.StoreID == s.StoreID)
					.Select (o => new {
						Date = o.Key.OrderDate,
						NumberOfOrders = o.Count(),
						ProductSales = Orders
									.Where (w => w.OrderDate == o.Key.OrderDate && w.StoreID == o.Key.StoreID)
									.Select ( x => x.SubTotal)
									.Sum(),
						GST = Orders
									.Where (w => w.OrderDate == o.Key.OrderDate && w.StoreID == o.Key.StoreID)
									.Select ( x => x.GST)
									.Sum(),
					})
	})
	.OrderBy (c => c.City)
	.ThenBy (l => l.Location)
	.Dump();

Orders
.Select (x => x).Dump();

/*Query 4
Print out all product items on a requested order (use Order #33). Group by Category and order by Product Description. 
You do not need to format money as this would be done at the presentation level. 
Use the QtyPicked in your calculations. Hint: You will need to using type casting (decimal). 
Use of the ternary operator will help.*/

Categories
	.GroupBy (c => new {c.Description, c.CategoryID})
	.OrderBy (o => o.Key.Description)
	.Select (p => new {
		Category = p.Key.Description,
		OrderProducts = OrderLists
							.Where (l => l.Product.CategoryID == p.Key.CategoryID) /*Onde: CategoryID da Tabela OrderLists é o mesmo da tabela
																					Categories que é a informação que ambas têm em comum para
																					que possam ser agrupadas*/
							.Where (o => o.OrderID == 33)
							.Select (op => new {
								Product = op.Product.Description,
								Price = op.Product.Price,
								PickedQty = op.QtyPicked,
								Discount = op.Discount,
								Subtotal = op.Product.Price * (decimal)op.QtyPicked,
								Tax = op.Product.Taxable == true ? ((op.Product.Price * (decimal)op.QtyPicked) - op.Discount) / op.Order.GST : 0, 
								ExtendedPrice = ((op.Product.Price * (decimal)op.QtyPicked + op.Order.GST)) 
							})//Tex = ol.Product.Taxable ? ((ol.Price.-ol.Discount) * (decimal)ol.QtyPicked) * .05m : 0
							//ExtendedPrice((ol.Price - ol.Discount) * (decimal)ol.Qtyicked) + (ol.Product.Taxable ? ((ol.Price - ol.Discount) * (decimal) ol.QtyPicked) * .05m : 0) 
	})
	.Dump();
	
/*Query 5
Generate a report on store orders and sales. Group this report by store. 
Show the total orders, the average order size (number of items per order) and average pre-tax revenue.*/

Orders
	.GroupBy (g => new {g.Store.Location})
	.OrderBy (ob => ob.Key.Location)
	.Select (o => new {
			Location = o.Key.Location,
			Orders = o.Count(),
			/*OrderSum = Orders
						.Where (w => w.Store.Location == o.Key.Location)
						.Sum(s => s.OrderLists.Count()),*/ //Conta-se quantas OrderLists tem em cada Ordem e soma-se
			AvgSize = Orders //Pega-se todas as Ordens
						//.Where (w => w.Store.Location == o.Key.Location) //Agrupa-se todas as Ordens por Localidade
						.Average (av => av.OrderLists.Count()), //Conta-se quantas Listas de Ordem se tem em cada Ordem e depois faz a média
																
			AvgRevenue = Orders
						.Where (w => w.Store.Location == o.Key.Location)
						.Average (av => av.SubTotal)
	})
	.Dump();

/*Orders
	.Select (o => o).Dump();*/
	
/*Orders
	.Where (w => w.Store.Location == "BerryPatch")
	.Select (o => new {
		OrderNumber = o.OrderID,
		OrderNumberCount = Orders
							.Where (w => w.Store.Location == "BerryPatch")
							.Count(),
		TotalItemsPerOrder = o.OrderLists.Count(), //contagem do número de itens em uma ordem
		TotalItemsSum = Orders
						.Where (w => w.Store.Location == "BerryPatch")
						.Sum(s => s.OrderLists.Count())
		
	}).Dump();*/

/*Query 6
List all the products a customer (use Customer #1) 
has purchased and the number of times the product was purchased. Order by number of times purchased then description.*/

Customers
	.Where (c => c.CustomerID == 1)
	.Select (l => new {
		Customer = (l.LastName + ',' + l.FirstName),
		OrdersCount = l.Orders.Count(),
		Items = OrderLists
				.GroupBy (g => new {g.Order.CustomerID, g.Product.Description}) //juntar os pedidos de um só cliente juntos
				.Where (o => o.Key.CustomerID == l.CustomerID)
				.Select (o => new {
					Description = o.Key.Description,
					TimesBought = o.Count()	
				})
				.OrderByDescending (ob => ob.TimesBought)
				.ThenBy (ob => ob.Description)
	})
	.Dump();
	
/*Customers
	.Where (c => c.CustomerID == 1)
	.Select (l => new {
		Customer = (l.LastName + ',' + l.FirstName),
		OrdersCount = l.Orders.Count(),
		Items = OrderLists
				.Where (o => o.Order.CustomerID == l.CustomerID) //ordens de um só cliente junto
				.GroupBy (g => g.Product) //agrupar por ProductID (PK) 
				.Select (o => new {
					Description = o.Key.Description,
					TimesBought = o.Count()	
				})
				.OrderByDescending (ob => ob.TimesBought)
				.ThenBy (ob => ob.Description)
	})
	.Dump();*/ //right answer not submited