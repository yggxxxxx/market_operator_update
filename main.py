import os
import pandas as pd

from tariff import load_tou_profile, load_fit_profile
from zip_strategy import ZIPStrategy
from order_book import Order, OrderBook
from cda import CDA_mechanism


TARIFF_TARGET_YEAR = int(os.getenv("TARIFF_TARGET_YEAR", "2025"))
SIM_SEASON = os.getenv("SIM_SEASON", "").strip().lower() or None
TARIFF_AGG = os.getenv("TARIFF_AGG", "median").strip().lower()


def gen_orders_and_slot(
    slot_df,
    tou_profile,
    fit_profile,
    trader_registry,
    order_id_start=0
):
    order_book = OrderBook()
    order_counter = order_id_start

    slot_df = slot_df.sort_values(["DateTime", "h_id"]).reset_index(drop=True)

    for _, row in slot_df.iterrows():
        h_id = int(row["h_id"])
        DateTime = row["DateTime"]
        hour = pd.to_datetime(DateTime).hour

        import_energy = float(row["import_energy"])
        export_energy = float(row["export_energy"])

        fit_price = fit_profile.get_price(hour)
        tou_price = tou_profile.get_price(hour)

        if import_energy > 0:
            side = "buy"
            quantity = import_energy
            limit_price = tou_price
        elif export_energy > 0:
            side = "sell"
            quantity = export_energy
            limit_price = fit_price
        else:
            continue

        trader_key = (h_id, side)

        if trader_key not in trader_registry:
            trader_registry[trader_key] = ZIPStrategy(
                h_id=h_id,
                side=side,
            )

        strategy = trader_registry[trader_key]

        submitted_price = strategy.generate_shout(
            fit_price=fit_price,
            tou_price=tou_price
        )

        order_counter += 1
        order_id = f"O{order_counter}"

        order = Order(
            order_id=order_id,
            h_id=h_id,
            trader_key=trader_key,
            DateTime=DateTime,
            hour=hour,
            side=side,
            quantity=quantity,
            remaining_quantity=quantity,
            limit_price=limit_price,
            submitted_price=submitted_price,
        )
        order_book.add_order(order)

    return order_book, trader_registry, order_counter


def print_test_households(test_df):
    print("=== TEST HOUSEHOLDS ===")
    print(test_df[[
        "DateTime",
        "h_id",
        "demand",
        "pv",
        "battery_charged",
        "battery_discharged",
        "energy_before",
        "energy_after",
        "import_energy",
        "export_energy",
        "soc"
    ]])


def print_slot_summary(slot_df):
    num_buyers = int((slot_df["import_energy"] > 0).sum())
    num_sellers = int((slot_df["export_energy"] > 0).sum())

    print("\n=== SLOT SUMMARY ===")
    print(f"DateTime = {slot_df['DateTime'].iloc[0]}")
    print(f"num_buyers  = {num_buyers}")
    print(f"num_sellers = {num_sellers}")
    print(f"total_import_energy = {slot_df['import_energy'].sum():.3f} kWh")
    print(f"total_export_energy = {slot_df['export_energy'].sum():.3f} kWh")


def print_order_book(order_book):
    print("\n=== INITIAL ORDER BOOK ===")
    order_book.print_book()


def print_results(result):
    print("\n=== COMMITTED TRADES ===")
    if not result["committed_trades"]:
        print("[empty]")
    for trade in result["committed_trades"]:
        print(
            f"trade_id={trade.trade_id}, "
            f"buyer_h_id={trade.buyer_h_id}, seller_h_id={trade.seller_h_id}, "
            f"buyer_order_id={trade.buyer_order_id}, seller_order_id={trade.seller_order_id}, "
            f"DateTime={trade.DateTime}, hour={trade.hour}, "
            f"quantity={trade.quantity:.3f} kWh, "
            f"trade_price={trade.trade_price:.4f} GBP/kWh, "
            f"trade_value={trade.trade_value:.4f}, "
            f"trade_round={trade.trade_round}"
        )

    print("\n=== UNMATCHED ORDERS ===")
    if not result["unmatched_orders"]:
        print("[empty]")
    for order in result["unmatched_orders"]:
        print(
            f"unmatched_order_id={order.unmatched_order_id}, "
            f"order_id={order.order_id}, h_id={order.h_id}, "
            f"DateTime={order.DateTime}, hour={order.hour}, side={order.side}, "
            f"original_quantity={order.original_quantity:.3f} kwh, "
            f"remaining_quantity={order.remaining_quantity:.3f} kwh, "
            f"limit_price={order.limit_price:.4f} GBP/kWh, "
            f"submitted_price={order.submitted_price:.4f} GBP/kWh"
        )

    print("\n=== SUMMARY ===")
    print(f"num_trades = {result['num_trades']}")
    print(f"num_unmatched_orders = {result['num_unmatched_orders']}")


def run_one_slot(
    slot_df,
    tou_profile,
    fit_profile,
    trader_registry,
    order_id_start,
    verbose=True
):
    print_slot_summary(slot_df)

    order_book, trader_registry, order_id_end = gen_orders_and_slot(
        slot_df=slot_df,
        tou_profile=tou_profile,
        fit_profile=fit_profile,
        trader_registry=trader_registry,
        order_id_start=order_id_start,
    )

    print_order_book(order_book)

    mechanism = CDA_mechanism(
        order_book=order_book,
        trader_registry=trader_registry,
        max_trade_rounds=150,
        max_no_trade_rounds=50,
        verbose=verbose,
    )

    result = mechanism.run_cda()
    print_results(result)
    return result, trader_registry, order_id_end


def run_market_sessions(test_df, verbose=True):
    if test_df is None or test_df.empty:
        raise ValueError("test_df is empty")

    print_test_households(test_df)

    tou_profile = load_tou_profile(
        target_year=TARIFF_TARGET_YEAR,
        season=SIM_SEASON,
        agg=TARIFF_AGG,
    )
    fit_profile = load_fit_profile(
        target_year=TARIFF_TARGET_YEAR,
        season=SIM_SEASON,
        agg=TARIFF_AGG,
    )

    trader_registry = {}
    order_id_counter = 0
    slot_results = []

    grouped = test_df.groupby("DateTime", sort=True)

    for dt, slot_df in grouped:
        print("\n" + "#" * 90)
        print(f"RUNNING MARKET SESSION FOR SLOT: {dt}")
        print("#" * 90)

        result, trader_registry, order_id_counter = run_one_slot(
            slot_df=slot_df.reset_index(drop=True),
            tou_profile=tou_profile,
            fit_profile=fit_profile,
            trader_registry=trader_registry,
            order_id_start=order_id_counter,
            verbose=verbose,
        )

        slot_results.append({"DateTime": dt, "result": result})

    total_num_trades = sum(item["result"]["num_trades"] for item in slot_results)
    total_num_unmatched_orders = sum(
        item["result"]["num_unmatched_orders"] for item in slot_results
    )

    print("\n" + "=" * 90)
    print("OVERALL SUMMARY ACROSS ALL SLOTS")
    print("=" * 90)
    print(f"num_slots = {len(slot_results)}")
    print(f"total_num_trades = {total_num_trades}")
    print(f"total_num_unmatched_orders = {total_num_unmatched_orders}")

    return {
        "slot_results": slot_results,
        "total_num_trades": total_num_trades,
        "total_num_unmatched_orders": total_num_unmatched_orders,
        "trader_registry": trader_registry,
    }


def main(test_df, verbose=True):
    return run_market_sessions(test_df=test_df, verbose=verbose)


if __name__ == "__main__":
    from test_household import get_test_df

    test_df = get_test_df()
    main(test_df, verbose=False)