
package one.microstream.demo.bookstore;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.Range;

import one.microstream.demo.bookstore.data.Book;
import one.microstream.demo.bookstore.data.Books;
import one.microstream.demo.bookstore.data.Customer;
import one.microstream.demo.bookstore.data.Customers;
import one.microstream.demo.bookstore.data.Employee;
import one.microstream.demo.bookstore.data.Purchase;
import one.microstream.demo.bookstore.data.Purchases;
import one.microstream.demo.bookstore.data.RandomDataAmount;
import one.microstream.demo.bookstore.data.Shop;
import one.microstream.demo.bookstore.data.Shops;
import one.microstream.persistence.types.Storer;
import one.microstream.reference.LazyReferenceManager;
import spark.Spark;


public class PerformanceTest
{
	static AtomicBoolean running = new AtomicBoolean(true);
	static AtomicInteger threads = new AtomicInteger(0);
	
	
	public static void main(final String[] args)
	{
		final BookStoreDemo demo = new BookStoreDemo(RandomDataAmount.Medium());
		
		// custom channel count (used cores), must be a power of 2
        // demo.setChannelCount(32);
		
		initShutdownHook(demo);
		
		// force init
		demo.data();
				
		new Reader(demo).start();
		new Writer(demo).start();
		
	}
	
	static class Writer extends Thread
	{
		final BookStoreDemo demo;
		
		Writer(final BookStoreDemo demo)
		{
			super("Writer");
			this.demo = demo;
		}
		
		@Override
		public void run()
		{
			threads.incrementAndGet();

			final Random               random          = new Random();
			Storer                     storer          = this.demo.storageManager().createStorer();
			long                       timeMark        = System.currentTimeMillis();
			int                        transactionSize = 0;
			final IntSummaryStatistics stats           = new IntSummaryStatistics();
			
			while(running.get())
			{
				final int                 itemCount = 3;
				final List<Purchase.Item> items     = new ArrayList<>(itemCount);
				final Books               books     = this.demo.data().books();
				final Book                book      =
					books.compute(stream -> stream.skip(random.nextInt(books.bookCount())).findFirst().get());
				items.add(Purchase.Item.New(book, 1));
				
				final Shops     shops     = this.demo.data().shops();
				final Shop      shop      =
					shops.compute(stream -> stream.skip(shops.shopCount() / 2).findFirst().get());
				
				final Employee  employee  = shop.randomEmployee(random);
				
				final Customers customers = this.demo.data().customers();
				final Customer  customer  = customers
					.compute(stream -> stream.skip(customers.customerCount() / 2).findFirst().get());
				
				final Purchase  purchase  = Purchase.New(shop, employee, customer, LocalDateTime.now(), items);
				this.demo.data().purchases().addAndRemoveRandom(purchase, storer);
				transactionSize++;
				
				final long now            = System.currentTimeMillis();
				final long ellapsedMillis = now - timeMark;
				if(ellapsedMillis >= 1000L)
				{
					storer.commit();
					storer = this.demo.storageManager().createStorer();
					
					stats.accept(transactionSize);
					this.demo.logger().info(
						"Transactions: " + transactionSize + " in " + ellapsedMillis + ", avg=" + stats.getAverage()
					);
					
					timeMark        = now;
					transactionSize = 0;
				}
				
			}
			
			threads.decrementAndGet();
			
		}
	}
	
	static class Reader extends Thread
	{
		final BookStoreDemo demo;
		
		Reader(final BookStoreDemo demo)
		{
			super("Reader");
			this.demo = demo;
		}
		
		@SuppressWarnings("incomplete-switch")
		@Override
		public void run()
		{
			threads.incrementAndGet();
			
			int          type   = 0;
			final Random random = new Random();
			
			while(running.get())
			{
				final char letter = (char)('A' + random.nextInt(26));
				
				switch(type)
				{
					case 0:
					{
						this.demo.data().books().searchByTitle(letter + "*");
					}
						break;
					
					case 1:
					{
						this.demo.data().customers().compute(
							customers -> customers
								.filter(c -> c.address().city().state().country().name().startsWith("" + letter))
								.collect(Collectors.toList()));
					}
						break;
					
					case 2:
					{
						this.demo.data().shops().compute(shops -> shops
							.filter(s -> s.address().city().state().country().name().startsWith("" + letter))
							.collect(Collectors.toList()));
					}
						break;
					
					case 3:
					{
						final Purchases      purchases = this.demo.data().purchases();
						final Range<Integer> years     = purchases.years();
						final int            year      =
							years.lowerEndpoint() + random.nextInt(years.upperEndpoint() - years.lowerEndpoint() + 1);
						purchases.bestSellerList(year);
					}
						break;
					
					case 4:
					{
						final Purchases      purchases = this.demo.data().purchases();
						final Range<Integer> years     = purchases.years();
						final int            year      =
							years.lowerEndpoint() + random.nextInt(years.upperEndpoint() - years.lowerEndpoint() + 1);
						purchases.employeeOfTheYear(year);
					}
						break;
				}
				
				type++;
				if(type == 4)
				{
					type = 0;
				}
				
				try
				{
					Thread.sleep(100);
				}
				catch(final InterruptedException e)
				{
					// swallow
				}
				
			}
			
			threads.decrementAndGet();
			
		}
		
	}

	private static void initShutdownHook(final BookStoreDemo demo)
	{
		Spark.port(1337);
		Spark.get("/stop", (req, res) ->
		{
			demo.logger().info("Shutdown signal received");
			
			running.set(false);
			
			new Thread(() ->
			{
				do
				{
					try
					{
						Thread.sleep(500);
					}
					catch(final InterruptedException e)
					{
						// swallow
					}
				}
				while(threads.get() > 0);
				
				demo.shutdown();
				LazyReferenceManager.get().stop();
				Spark.stop();
				
				demo.logger().info("Good bye!");
				
			}).start();
			
			return "OK";
		});
	}
	
}
