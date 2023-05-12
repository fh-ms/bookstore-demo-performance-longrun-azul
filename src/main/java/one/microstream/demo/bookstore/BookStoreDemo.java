
package one.microstream.demo.bookstore;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;

import javax.money.CurrencyUnit;
import javax.money.Monetary;
import javax.money.MonetaryAmount;
import javax.money.format.AmountFormatQueryBuilder;
import javax.money.format.MonetaryAmountFormat;
import javax.money.format.MonetaryFormats;

import org.javamoney.moneta.RoundedMoney;
import org.javamoney.moneta.format.CurrencyStyle;
import org.rapidpm.dependencies.core.logger.HasLogger;

import one.microstream.demo.bookstore.data.Data;
import one.microstream.demo.bookstore.data.DataMetrics;
import one.microstream.demo.bookstore.data.RandomDataAmount;
import one.microstream.persistence.binary.jdk8.types.BinaryHandlersJDK8;
import one.microstream.storage.embedded.configuration.types.EmbeddedStorageConfiguration;
import one.microstream.storage.embedded.configuration.types.EmbeddedStorageConfigurationBuilder;
import one.microstream.storage.embedded.types.EmbeddedStorageFoundation;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;
import one.microstream.storage.types.StorageChannelCountProvider;


public final class BookStoreDemo implements HasLogger
{
	/**
	 * {@link CurrencyUnit} for this demo, US Dollar is used as only currency.
	 */
	private static final CurrencyUnit         CURRENCY_UNIT          = Monetary.getCurrency(Locale.US);

	/**
	 * Money format
	 */
	private final static MonetaryAmountFormat MONETARY_AMOUNT_FORMAT = MonetaryFormats.getAmountFormat(
		AmountFormatQueryBuilder.of(Locale.getDefault())
			.set(CurrencyStyle.SYMBOL)
			.build()
	);

	/**
	 * Multiplicant used to calculate retail prices, adds an 11% margin.
	 */
	private final static BigDecimal           RETAIL_MULTIPLICANT    = scale(new BigDecimal(1.11));


	private static BigDecimal scale(final BigDecimal number)
	{
		return number.setScale(2, RoundingMode.HALF_UP);
	}

	/**
	 * @return the {@link CurrencyUnit} for this demo, US Dollar is used as only currency.
	 */
	public static CurrencyUnit currencyUnit()
	{
		return CURRENCY_UNIT;
	}

	/**
	 * @return the {@link MonetaryAmountFormat} for this demo
	 */
	public static MonetaryAmountFormat monetaryAmountFormat()
	{
		return MONETARY_AMOUNT_FORMAT;
	}

	/**
	 * Converts a double into a {@link MonetaryAmount}
	 * @param number the number to convert
	 * @return the converted {@link MonetaryAmount}
	 */
	public static MonetaryAmount money(final double number)
	{
		return money(new BigDecimal(number));
	}

	/**
	 * Converts a {@link BigDecimal} into a {@link MonetaryAmount}
	 * @param number the number to convert
	 * @return the converted {@link MonetaryAmount}
	 */
	public static MonetaryAmount money(final BigDecimal number)
	{
		return RoundedMoney.of(scale(number), currencyUnit());
	}

	/**
	 * Calculates the retail price based on a purchase price by adding a margin.
	 * @param purchasePrice the purchase price
	 * @return the calculated retail price
	 * @see #RETAIL_MULTIPLICANT
	 */
	public static MonetaryAmount retailPrice(
		final MonetaryAmount purchasePrice
	)
	{
		return money(RETAIL_MULTIPLICANT.multiply(new BigDecimal(purchasePrice.getNumber().doubleValue())));
	}


	private final    RandomDataAmount       initialDataAmount;
	private          int                    channelCount     ;
	private volatile EmbeddedStorageManager storageManager   ;

	/**
	 * Creates a new demo instance.
	 *
	 * @param initialDataAmount the amount of data which should be generated if the database is empty
	 */
	public BookStoreDemo(final RandomDataAmount initialDataAmount)
	{
		super();
		this.initialDataAmount = initialDataAmount;
		this.channelCount = Math.max(
			1, // minimum one channel, if only 1 core is available
			Integer.highestOneBit(Runtime.getRuntime().availableProcessors() - 1)
		);
	}
	
	public void setChannelCount(final int channelCount)
	{
		StorageChannelCountProvider.Validation.validateParameters(channelCount);
		
		this.channelCount = channelCount;
	}
	
	public int getChannelCount()
	{
		return this.channelCount;
	}

	/**
	 * Gets the lazily initialized {@link EmbeddedStorageManager} used by this demo.
	 * If no storage data is found, a {@link Data} root object is generated randomly,
	 * based on the given {@link RandomDataAmount}.
	 *
	 * @return the MicroStream {@link EmbeddedStorageManager} used by this demo
	 */
	public EmbeddedStorageManager storageManager()
	{
		/*
		 * Double-checked locking to reduce the overhead of acquiring a lock
		 * by testing the locking criterion.
		 * The field (this.storageManager) has to be volatile.
		 */
		if(this.storageManager == null)
		{
			synchronized(this)
			{
				if(this.storageManager == null)
				{
					this.storageManager = this.createStorageManager();
				}
			}
		}

		return this.storageManager;
	}

	/**
	 * Creates an {@link EmbeddedStorageManager} and initializes random {@link Data} if empty.
	 */
	private EmbeddedStorageManager createStorageManager()
	{
		final long start = System.currentTimeMillis();
		
		this.logger().info("Initializing MicroStream StorageManager");

		final EmbeddedStorageConfigurationBuilder configuration = EmbeddedStorageConfiguration.Builder()
			.setStorageDirectory("data/storage")
			.setChannelCount(this.channelCount)
		;

		final EmbeddedStorageFoundation<?> foundation = configuration.createEmbeddedStorageFoundation();
		foundation.onConnectionFoundation(BinaryHandlersJDK8::registerJDK8TypeHandlers);
		final EmbeddedStorageManager storageManager = foundation.createEmbeddedStorageManager().start();

		if(storageManager.root() == null)
		{
			this.logger().info("No data found, initializing random data");

			final Data.Default data = Data.New();
			storageManager.setRoot(data);
			storageManager.storeRoot();
			final DataMetrics metrics = data.populate(
				this.initialDataAmount,
				storageManager
			);

			this.logger().info("Random data generated: " + metrics.toString());
		}
		
		this.logger().info("Initialized in " + (System.currentTimeMillis() - start) +  " ms");

		return storageManager;
	}

	/**
	 * Gets the {@link Data} root object of this demo.
	 * This is the entry point to all of the data used in this application, basically the "database".
	 *
	 * @return the {@link Data} root object of this demo
	 */
	public Data data()
	{
		return (Data)this.storageManager().root();
	}

	/**
	 * Shuts down the {@link EmbeddedStorageManager} of this demo.
	 */
	public synchronized void shutdown()
	{
		if(this.storageManager != null)
		{
			this.storageManager.shutdown();
			this.storageManager = null;
		}
	}

}
