from business.yahoo_finance import YahooFinanceConsumer


def main() -> None:
    YahooFinanceConsumer().start_consumer()


if __name__ == "__main__":
    main()
