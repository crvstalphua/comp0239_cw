import argparse
import os
import glob

def parse_args():
    parser = argparse.ArgumentParser(description="View query results")
    parser.add_argument("--query", help="Query name from submit_query.py")
    parser.add_argument("--rows", type=int, default=20, help="Number of rows to show")
    parser.add_argument("--list", action="store_true", help="List all completed queries")
    return parser.parse_args()

def list_queries():
    queries = sorted(glob.glob("/data/nyc-taxi/outputs/query_*"))
    if not queries:
        print("No completed queries found")
        return
    print(f"{'Query name':<50} {'Status'}")
    print("-" * 60)
    for q in queries:
        name = os.path.basename(q)
        has_results = bool(glob.glob(f"{q}/part-*.csv"))
        status = "complete" if has_results else "no results"
        print(f"{name:<50} {status}")

def show_results(query_name, rows):
    output_dir = f"/data/nyc-taxi/outputs/{query_name}"

    if not os.path.exists(output_dir):
        print(f"Error: {query_name} not found")
        print("Run with --list to see available queries")
        return

    csv_files = glob.glob(f"{output_dir}/part-*.csv")
    if not csv_files:
        print(f"No results found in {output_dir}")
        return
    
    print(f"Results for: {query_name}")
    print(f"{'distance':>10} {'pickup zone':>12} {'dropoff zone':>13} {'passengers':>11} {'predicted fare':>15}")
    print("-" * 60)

    count = 0
    with open(csv_files[0]) as f:
        next(f)
        for line in f:
            if count >= rows:
                break
            parts = line.strip().split(",")
            if len(parts) >= 5:
                try:
                    print(f"{float(parts[0]):>10.1f} {parts[1]:>12} {parts[2]:>13} {parts[3]:>11} ${float(parts[4]):>14.2f}")
                except ValueError:
                    continue

            count += 1

    print(f"\nShowing {count} rows. Full results at: {output_dir}")

def main():
    args = parse_args()

    if args.list:
        list_queries()
    elif args.query:
        show_results(args.query, args.rows)
    else:
        print("Error: provide --query <name> or --list")
        print("Example: python3 get_results.py --list")

if __name__ == "__main__":
    main()