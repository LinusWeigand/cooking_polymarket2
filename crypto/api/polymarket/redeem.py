from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from web3 import Web3
import json
import os

from crypto.api.polymarket.account import get_client, get_my_open_positions

# CTF Contract address on Polygon
CTF_CONTRACT_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# CTF Contract ABI (simplified for redeemPositions function)
CTF_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]


def redeem_redeemable_positions(client, open_positions, web3_provider_url):
    """
    Redeem all redeemable positions from the open_positions output

    Args:
        client: ClobClient instance
        open_positions: List of position dictionaries from get_my_open_positions()
        web3_provider_url: Polygon RPC URL
    """
    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(web3_provider_url))

    # USDC contract address on Polygon
    USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

    # Get the CTF contract
    ctf_contract = w3.eth.contract(address=CTF_CONTRACT_ADDRESS, abi=CTF_ABI)

    # Group redeemable positions by conditionId
    redeemable_by_condition = {}

    for position in open_positions:
        if position.get('redeemable', False) and position.get('currentValue', 0) > 0:
            condition_id = position['conditionId']

            if condition_id not in redeemable_by_condition:
                redeemable_by_condition[condition_id] = []

            redeemable_by_condition[condition_id].append(position)

    print(f"Found {len(redeemable_by_condition)} conditions with redeemable positions")

    # Redeem positions for each condition
    for condition_id, positions in redeemable_by_condition.items():
        try:
            print(f"\nRedeeming positions for condition: {condition_id}")

            # For binary markets, indexSets are [1, 2] representing [YES, NO]
            index_sets = [1, 2]

            # Build transaction
            txn = ctf_contract.functions.redeemPositions(
                USDC_ADDRESS,  # collateralToken (USDC)
                b'\x00' * 32,  # parentCollectionId (null for Polymarket)
                condition_id,  # conditionId
                index_sets  # indexSets [1, 2] for binary markets
            ).build_transaction({
                'from': client.get_address(),  # Your wallet address
                'gas': 200000,  # Adjust gas limit as needed
                'gasPrice': w3.to_wei('30', 'gwei'),  # Adjust gas price
                'nonce': w3.eth.get_transaction_count(client.get_address()),
            })
            load_dotenv()
            private_key = os.getenv("PRIVATE_KEY")

            # Remove 0x prefix if present
            if private_key.startswith("0x"):
                private_key = private_key[2:]

            # Sign and send transaction
            signed_txn = w3.eth.account.sign_transaction(txn, private_key=private_key)
            tx_hash = w3.eth.send_raw_transaction(signed_txn.raw_transaction)

            print(f"Redemption transaction sent: {tx_hash.hex()}")

            # Wait for confirmation
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash)

            if receipt.status == 1:
                print(f"✅ Successfully redeemed positions for condition {condition_id}")

                # Calculate total redeemed value
                total_value = sum(pos.get('currentValue', 0) for pos in positions)
                print(f"Total value redeemed: ${total_value:.2f}")
            else:
                print(f"❌ Transaction failed for condition {condition_id}")

        except Exception as e:
            print(f"❌ Error redeeming condition {condition_id}: {e}")
            continue


def redeem_all_redeemable(client, web3_provider_url="https://polygon-rpc.com"):
    """
    Convenience function to get positions and redeem all redeemable ones
    """
    # Get current positions
    positions = get_my_open_positions(client)

    if not positions:
        print("No positions found")
        return

    # Filter for redeemable positions
    redeemable_positions = [pos for pos in positions if pos.get('redeemable', False)]

    if not redeemable_positions:
        print("No redeemable positions found")
        return

    print(f"Found {len(redeemable_positions)} redeemable positions")

    # Redeem them
    redeem_redeemable_positions(client, redeemable_positions, web3_provider_url)


# Usage example:
if __name__ == '__main__':
    client = get_client()

    # Option 1: Redeem all redeemable positions automatically
    redeem_all_redeemable(client)

    # Option 2: Use your existing positions data
    # positions = get_my_open_positions(client)
    # redeem_redeemable_positions(client, positions, "https://polygon-rpc.com")