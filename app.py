# app.py

import datetime
import os
from flask import Flask, request, jsonify, render_template, redirect, url_for, session, flash
from flask_cors import CORS
from supabase import create_client, Client
from dotenv import load_dotenv
import razorpay
import hmac
import hashlib
import uuid
from werkzeug.security import check_password_hash, generate_password_hash
from postgrest.exceptions import APIError
import logging
from functools import wraps # Ensure this is imported for @wraps
import uuid # 
import requests
# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app_logger = logging.getLogger(__name__)

# Load environment variables from .env file FIRST
load_dotenv()

app = Flask(__name__)
CORS(app)

# --- REQUIRED FOR ADMIN SESSION AUTHENTICATION ---
# IMPORTANT: Change this to a strong, random value and store it in an environment variable!
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "your_highly_secret_and_random_key_here_please_change_this")

# Admin credentials (for demo purposes ONLY - use environment variables or a proper DB in production!)
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin_user")
# Generate hash only once at startup
ADMIN_PASSWORD_HASH = generate_password_hash(os.environ.get("ADMIN_PASSWORD", "admin_pass"))

# Initialize Supabase Admin client
supabase: Client = None
supabase_admin_auth = None
try:
    SUPABASE_URL: str = os.environ.get("SUPABASE_URL")
    SUPABASE_SERVICE_ROLE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    supabase_admin_auth = supabase.auth.admin
    app_logger.info("Supabase admin client initialized successfully.")
except Exception as e:
    app_logger.error(f"Error initializing Supabase client: {e}")
    supabase = None
    supabase_admin_auth = None

## Initialize Razorpay Client (for payments ONLY)
razorpay_client = None
try:
    RAZORPAY_KEY_ID: str = os.environ.get("RAZORPAY_KEY_ID")
    RAZORPAY_KEY_SECRET: str = os.environ.get("RAZORPAY_KEY_SECRET")

    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        raise ValueError("RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET must be set in .env")

    razorpay_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
    # REMOVE OR COMMENT OUT THIS LINE:
    # razorpay_client.set_app_details(app_name="VoltEarning", app_version="1.0")
    app_logger.info("Razorpay client (for payments) initialized successfully.")
except Exception as e:
    app_logger.error(f"Error initializing Razorpay client: {e}")
    razorpay_client = None

FRONTEND_SIGNUP_BASE_URL = os.environ.get("FRONTEND_SIGNUP_BASE_URL", "https://volt-earning.vercel.app/signup")

# --- DECORATOR for Admin Authentication ---
def admin_required(f):
    @wraps(f) # This preserves original function's metadata
    def decorated_function(*args, **kwargs):
        if not session.get('admin_logged_in'):
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated_function


# --- CORE LOGIC FUNCTION: update_withdrawal_status_logic ---
# This function contains the actual logic for updating transaction status
# and handling refunds. It is called by both the external API route and the internal admin route.
def update_withdrawal_status_logic(data):
    try:
        transaction_id = data.get('transaction_id')
        new_status = data.get('status')
        admin_notes = data.get('admin_notes', '')

        if not all([transaction_id, new_status]):
            return jsonify({'success': False, 'message': 'Transaction ID and new status are required.'}), 400

        if new_status not in ['completed', 'rejected', 'failed']:
            return jsonify({'success': False, 'message': 'Invalid status. Allowed: completed, rejected, failed.'}), 400

        update_payload = {
            'status': new_status,
            'admin_notes': admin_notes,
            'updated_at': datetime.datetime.now().isoformat()
        }

        # Handle refund if the status is rejected or failed
        if new_status in ['rejected', 'failed']:
            if not supabase:
                app_logger.error("Supabase client not initialized, cannot process refund for rejected/failed withdrawal.")
                return jsonify({'success': False, 'message': 'Backend error: Database connection issue for refund.'}), 500

            # Fetch the original transaction to get user_id and amount
            transaction_response = supabase.table('transactions') \
                                   .select('user_id, amount') \
                                   .eq('id', transaction_id) \
                                   .single() \
                                   .execute()

            if transaction_response.data:
                user_id = transaction_response.data['user_id']
                amount_to_refund = transaction_response.data['amount']

                # Fetch and update the user's wallet balance
                wallet_response = supabase.table('user_wallets') \
                                  .select('balance') \
                                  .eq('user_id', user_id) \
                                  .single() \
                                  .execute()
                if wallet_response.data:
                    current_balance = wallet_response.data['balance']
                    refunded_balance = current_balance + amount_to_refund
                    supabase.table('user_wallets') \
                            .update({'balance': refunded_balance}) \
                            .eq('user_id', user_id) \
                            .execute()
                    app_logger.info(f"Refunded {amount_to_refund} to user {user_id} for failed/rejected withdrawal {transaction_id}.")
                else:
                    app_logger.warning(f"Could not find wallet for user {user_id} to refund for transaction {transaction_id}.")
            else:
                app_logger.warning(f"Could not find transaction {transaction_id} to get user_id for refund.")

        # Update the transaction status in the database
        response = supabase.table('transactions') \
                   .update(update_payload) \
                   .eq('id', transaction_id) \
                   .execute()

        if response and response.data and len(response.data) > 0:
            app_logger.info(f"Transaction {transaction_id} status updated to {new_status} by admin.")
            return jsonify({'success': True, 'message': f'Withdrawal request {transaction_id} updated to {new_status}.'}), 200
        else:
            app_logger.error(f"Failed to update transaction status for {transaction_id}. Response: {response.error if hasattr(response, 'error') and response.error else 'No data returned'}")
            return jsonify({'success': False, 'message': 'Failed to update withdrawal request status.'}), 500

    except Exception as e:
        app_logger.error(f"Error updating withdrawal status: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred while updating status.'}), 500


# --- HELPER FUNCTION: update_withdrawal_status_internal ---
# This function is specifically designed to be called internally from other Flask routes
# to trigger the core withdrawal status update logic.
def update_withdrawal_status_internal(req):
    # Directly calls the core logic function, passing the relevant data from the "simulated request"
    return update_withdrawal_status_logic(req.json)


# --- ADMIN LOGIN & LOGOUT ROUTES ---
@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if session.get('admin_logged_in'):
        return redirect(url_for('admin_dashboard'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        if username == ADMIN_USERNAME and check_password_hash(ADMIN_PASSWORD_HASH, password):
            session['admin_logged_in'] = True
            flash('Logged in successfully!', 'success')
            return redirect(url_for('admin_dashboard'))
        else:
            flash('Invalid credentials. Please try again.', 'danger')
    return render_template('admin_login.html')

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    flash('You have been logged out.', 'info')
    return redirect(url_for('admin_login'))


# --- ADMIN DASHBOARD ROUTES ---
@app.route('/admin/withdrawals', methods=['GET'])
@admin_required
def admin_dashboard():
    if not supabase:
        flash('Supabase client not initialized.', 'danger')
        return render_template('admin_withdrawals.html', requests=[], error='Backend setup issue.')

    try:
        # Fetch pending withdrawal requests
        # We also select 'profiles.nickname' to show user nickname
        response = supabase.table('transactions') \
            .select('*, bank_cards!inner(*), profiles!inner(nickname)') \
            .eq('type', 'withdrawal') \
            .eq('status', 'pending') \
            .order('created_at', desc=True) \
            .execute()

        if response.data:
            pending_requests = []
            for item in response.data:
                # Assuming metadata contains account details for manual transfers
                metadata = item.get('metadata', {})
                # Ensure bank_cards data is present
                bank_card_data = item.get('bank_cards')
                if bank_card_data:
                    account_holder_name = bank_card_data.get('account_holder_name', 'N/A')
                    account_number = bank_card_data.get('account_number', 'N/A')
                    ifsc_code = bank_card_data.get('ifsc_code', 'N/A')
                    bank_name = bank_card_data.get('bank_name', 'N/A')
                else:
                    # Fallback to metadata if bank_cards is not linked or missing for some reason
                    account_holder_name = metadata.get('account_holder_name', 'N/A')
                    account_number = metadata.get('account_number', 'N/A')
                    ifsc_code = metadata.get('ifsc_code', 'N/A')
                    bank_name = metadata.get('bank_name', 'N/A')

                pending_requests.append({
                    'id': item['id'],
                    'user_id': item['user_id'],
                    'nickname': item['profiles']['nickname'], # Get nickname from joined table
                    'amount': item['amount'],
                    'status': item['status'],
                    'created_at': item['created_at'],
                    'account_holder_name': account_holder_name,
                    'account_number': account_number,
                    'ifsc_code': ifsc_code,
                    'bank_name': bank_name,
                    'admin_notes': item.get('admin_notes', '') # Include existing admin notes
                })
            return render_template('admin_withdrawals.html', requests=pending_requests)
        else:
            return render_template('admin_withdrawals.html', requests=[], message='No pending withdrawal requests.')

    except Exception as e:
        app_logger.error(f"Error fetching pending withdrawals for admin dashboard: {e}", exc_info=True)
        flash(f'Error fetching withdrawals: {e}', 'danger')
        return render_template('admin_withdrawals.html', requests=[], error='Error fetching data.')

@app.route('/admin/withdrawals/process', methods=['POST'], endpoint='process_withdrawals')
@admin_required
def process_withdrawal_action():
    transaction_id = request.form.get('transaction_id')
    action = request.form.get('action') # 'complete' or 'reject'
    admin_notes = request.form.get('admin_notes_from_form', '') # Correctly get from the hidden input field

    if not all([transaction_id, action]):
        flash('Missing transaction ID or action.', 'danger')
        return redirect(url_for('admin_dashboard'))

    # Map action to status for the API endpoint
    new_status = 'completed' if action == 'complete' else 'rejected' if action == 'reject' else None

    if not new_status:
        flash('Invalid action specified.', 'danger')
        return redirect(url_for('admin_dashboard'))

    try:
        # Simulate the request object for the function
        simulated_request = type('obj', (object,), {'json': {
            'transaction_id': transaction_id,
            'status': new_status,
            'admin_notes': admin_notes
        }})()

        # Call the internal helper, which then calls the core logic
        response_tuple = update_withdrawal_status_internal(simulated_request)

        # Check the actual JSON response for success/failure (response_tuple is a (jsonify_obj, status_code) tuple)
        # Access the JSON content via .json property of the Flask Response object
        if response_tuple and isinstance(response_tuple, tuple) and response_tuple[0].json.get('success'):
            flash(response_tuple[0].json.get('message', f'Withdrawal {action}d successfully!'), 'success')
        elif response_tuple and isinstance(response_tuple, tuple):
             flash(response_tuple[0].json.get('message', f'Failed to {action} withdrawal.'), 'danger')
        else:
            flash(f'An unexpected response format was received after attempting to {action} withdrawal.', 'danger')

    except Exception as e:
        app_logger.error(f"Error processing withdrawal action: {e}", exc_info=True)
        flash(f'An unexpected error occurred: {e}', 'danger')

    return redirect(url_for('admin_dashboard'))


@app.route('/admin/recharge')
def admin_recharge_dashboard():
    """
    Renders the admin dashboard for manual recharge verification.
    This route must be protected with admin authentication.
    """
    return render_template('recharge_admin.html')


# --- EXISTING ROUTES (as provided in your context) ---

@app.route('/api/create-supabase-user', methods=['POST'])
def create_supabase_user():
    if not supabase_admin_auth or not supabase: # Ensure both are initialized
        app_logger.error("Supabase client not initialized in create_supabase_user.")
        return jsonify({'error': 'Backend setup issue: Supabase client not initialized'}), 500

    data = request.get_json()
    nickname = data.get('nickname')
    phone_number = data.get('phoneNumber')
    password = data.get('password')
    # New: referral_code
    referral_code_used = data.get('referral_code')
    print(f"Backend received referral_code: {referral_code_used}")
    if not all([nickname, phone_number, password]):
        return jsonify({'error': 'Nickname, phone number, and password are required.'}), 400

    referrer_id = None
    if referral_code_used:
        try:
            # Find the user who owns this referral code
            referrer_response = supabase.table('profiles') \
                                .select('id') \
                                .eq('referral_code', referral_code_used) \
                                .single() \
                                .execute()
            if referrer_response.data:
                referrer_id = referrer_response.data['id']
                app_logger.info(f"Referral code '{referral_code_used}' found, referrer ID: {referrer_id}")
            else:
                app_logger.warning(f"Invalid referral code used: {referral_code_used}")
                # Don't block registration, just don't assign referrer
        except Exception as e:
            app_logger.error(f"Error checking referral code {referral_code_used}: {e}", exc_info=True)
            # Continue without referrer if lookup fails

    try:
        user_response = supabase_admin_auth.create_user(
            {
                "phone": phone_number,
                "password": password,
                "phone_confirm": True
            }
        )

        if user_response.user is None:
            error_message_detail = user_response.dict().get('msg', 'Unknown error during user creation.')
            app_logger.error(f"Supabase create_user raw response: {user_response}")
            if 'User already exists' in error_message_detail:
                return jsonify({'error': 'This phone number is already registered. Please sign in.'}), 409
            return jsonify({'error': f'Failed to create user: {error_message_detail}'}), 500

        user_id = user_response.user.id
        app_logger.info(f"User created in auth.users: {user_id}")

        # Generate a unique referral code for the new user
        new_user_referral_code = str(uuid.uuid4()).replace('-', '')[:10].upper() # Example: 10-char UUID based
        
        profile_response = supabase.table("profiles").insert(
            {
                "id": user_id,
                "nickname": nickname,
                "phone_number": phone_number,
                "referral_code": new_user_referral_code, # Store the new user's referral code
                "referrer_id": referrer_id # Store the ID of the user who referred this new user
            }
        ).execute()

        if profile_response.data is None or len(profile_response.data) == 0:
            app_logger.error(f"Supabase profile insertion error: {profile_response.error if hasattr(profile_response, 'error') else 'No data or error'}")
            supabase_admin_auth.delete_user(user_id)
            return jsonify({'error': 'Failed to create user profile.'}), 500

        wallet_response = supabase.table("user_wallets").insert(
            {
                "user_id": user_id,
                "balance": 0.0,
                "total_income": 0.0,
                "pending_referral_bonus": 0.0, # Initialize new column
                "total_referral_earnings": 0.0 # Initialize new column
            }
        ).execute()

        if wallet_response.data is None or len(wallet_response.data) == 0:
            app_logger.error(f"Supabase wallet creation error: {wallet_response.error if hasattr(wallet_response, 'error') else 'No data or error'}")
            supabase_admin_auth.delete_user(user_id)
            # Attempt to delete profile if wallet creation fails
            try:
                supabase.table("profiles").delete().eq("id", user_id).execute()
            except Exception as delete_e:
                app_logger.error(f"Failed to clean up profile for {user_id} after wallet creation error: {delete_e}")
            return jsonify({'error': 'Failed to create user wallet.'}), 500

        # If a referrer exists, credit them with the instant bonus
        if referrer_id:
            # Increment pending_referral_bonus for the referrer
            try:
                # Use a transaction-like update for safety if Supabase supports it,
                # otherwise, fetch current and then update.
                # For simplicity, fetching current and updating.
                referrer_wallet = supabase.table('user_wallets') \
                                   .select('pending_referral_bonus') \
                                   .eq('user_id', referrer_id) \
                                   .single() \
                                   .execute()
                if referrer_wallet.data:
                    current_pending = referrer_wallet.data['pending_referral_bonus']
                    new_pending = current_pending + 10.0 # ₹10 bonus per sign-up
                    supabase.table('user_wallets') \
                            .update({'pending_referral_bonus': new_pending}) \
                            .eq('user_id', referrer_id) \
                            .execute()
                    app_logger.info(f"Credited ₹10 pending bonus to referrer {referrer_id} for new user {user_id}")
            except Exception as bonus_e:
                app_logger.error(f"Error crediting pending bonus to referrer {referrer_id}: {bonus_e}", exc_info=True)
                # This error should ideally not block new user creation, but should be logged.

        # Initialize user_quests for the new user (or just the referrer)
        # It's better to create quests for the referrer only.
        # However, if you want users to see their own quests even if not referring yet,
        # you could initialize them here too. For now, we'll assume quests are managed
        # based on referrer actions or fetched dynamically.

        app_logger.info(f"Profile and Wallet created for user: {user_id}")
        return jsonify({'message': 'Account created successfully!', 'userId': user_id, 'referralCode': new_user_referral_code}), 200

    except Exception as e:
        app_logger.error(f"An unexpected error occurred in backend: {e}", exc_info=True)
        return jsonify({'error': 'An unexpected error occurred on the server.'}), 500
    

# --- NEW: Get Invite Page Data ---
@app.route('/api/user/invite-data/<user_id>', methods=['GET'])
def get_invite_data(user_id):
    if not supabase:
        app_logger.error("Supabase client not initialized in get_invite_data.")
        return jsonify({'error': 'Backend setup issue: Supabase client not initialized'}), 500

    try:
        # 1. Fetch user's profile and wallet data
        user_data_response = supabase.table('profiles') \
                                     .select('referral_code, user_wallets(pending_referral_bonus, total_referral_earnings)') \
                                     .eq('id', user_id) \
                                     .single() \
                                     .execute()

        if not user_data_response.data:
            return jsonify({'success': False, 'message': 'User data not found.'}), 404

        profile_data = user_data_response.data
        wallet_data = profile_data.get('user_wallets')
        if not wallet_data:
            app_logger.error(f"Wallet data missing for user {user_id}")
            return jsonify({'success': False, 'message': 'User wallet data not found.'}), 500

        referral_code = profile_data.get('referral_code')
        # Ensure referral_code is generated if for some reason it's missing (shouldn't happen with current logic)
        if not referral_code:
            referral_code = str(uuid.uuid4()).replace('-', '')[:10].upper()
            supabase.table('profiles').update({'referral_code': referral_code}).eq('id', user_id).execute()

        pending_bonus = wallet_data.get('pending_referral_bonus', 0.0)
        total_referral_earnings = wallet_data.get('total_referral_earnings', 0.0)
        
        # 2. Count total direct referrals (users whose referrer_id is this user's ID)
        referred_users_response = supabase.table('profiles') \
                                         .select('id') \
                                         .eq('referrer_id', user_id) \
                                         .execute()
        
        total_referrals = len(referred_users_response.data) if referred_users_response.data else 0

        # 3. Count activated referrals
        direct_referral_ids = [user['id'] for user in referred_users_response.data] if referred_users_response.data else []
        
        activated_referrals_count = 0
        if direct_referral_ids:
            recharge_transactions_response = supabase.table('transactions') \
                                                     .select('user_id', count='exact') \
                                                     .in_('user_id', direct_referral_ids) \
                                                     .eq('type', 'recharge') \
                                                     .eq('status', 'completed') \
                                                     .execute()
            
            activated_user_ids = set()
            if recharge_transactions_response.data:
                for tx in recharge_transactions_response.data:
                    activated_user_ids.add(tx['user_id'])
            
            activated_referrals_count = len(activated_user_ids)

        # 4. Construct the final response without quest bonuses
        return jsonify({
            'success': True,
            'referralCode': referral_code,
            'invitationLink': f"{FRONTEND_SIGNUP_BASE_URL}?ref={referral_code}",
            'totalReferrals': total_referrals,
            'currentInvites': activated_referrals_count,
            'referralEarnings': total_referral_earnings,
            'pendingBonus': pending_bonus,
            'canClaimBonus': pending_bonus > 0,
        }), 200

    except Exception as e:
        app_logger.error(f"Error fetching invite data for user {user_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred while fetching invite data.'}), 500

@app.route('/api/user/team-data/<user_id>', methods=['GET'])
def get_team_data(user_id):
    if not supabase:
        app_logger.error("Supabase client not initialized in get_team_data.")
        return jsonify({'error': 'Backend setup issue: Supabase client not initialized'}), 500

    try:
        # 1. Fetch total referrals and member data
        referred_users_response = supabase.table('profiles') \
            .select('id, nickname, phone_number') \
            .eq('referrer_id', user_id) \
            .execute()
        
        referred_users = referred_users_response.data if referred_users_response.data else []
        total_referrals = len(referred_users)

        # 2. Fetch total earnings from the user's wallet
        wallet_data_response = supabase.table('user_wallets') \
            .select('total_referral_earnings') \
            .eq('user_id', user_id) \
            .single() \
            .execute()
        
        total_earnings = 0
        if wallet_data_response.data:
            total_earnings = wallet_data_response.data.get('total_referral_earnings', 0.0)

        # 3. Process the list of team members and determine their status
        team_members_list = []
        for member in referred_users:
            member_id = member['id']

            # --- MODIFIED LOGIC HERE ---
            # Check for completed transactions in the 'transactions' table (Razorpay)
            recharge_tx_response = supabase.table('transactions') \
                .select('id') \
                .eq('user_id', member_id) \
                .eq('type', 'recharge') \
                .eq('status', 'completed') \
                .limit(1) \
                .execute()
            
            is_active = len(recharge_tx_response.data) > 0

            # If no Razorpay transaction found, check the 'manual_payments' table
            if not is_active:
                manual_payment_response = supabase.table('manual_payments') \
                    .select('id') \
                    .eq('user_id', member_id) \
                    .eq('status', 'completed') \
                    .limit(1) \
                    .execute()
                
                is_active = len(manual_payment_response.data) > 0
            # --- END MODIFIED LOGIC ---
            
            team_members_list.append({
                'name': member.get('nickname', 'Unnamed User'),
                'phone': f"{member.get('phone_number', '')[:5]}****{member.get('phone_number', '')[-2:]}",
                'status': 'active' if is_active else 'inactive'
            })

        # 4. Construct and return the final response
        return jsonify({
            'success': True,
            'totalReferrals': total_referrals,
            'totalEarnings': total_earnings,
            'teamMembers': team_members_list
        }), 200

    except Exception as e:
        app_logger.error(f"Error fetching team data for user {user_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred while fetching team data.'}), 500

# --- MODIFIED: Claim Referral Bonus (₹10 per sign-up) ---
@app.route('/api/user/claim-referral-bonus', methods=['POST'])
def claim_referral_bonus():
    if not supabase:
        app_logger.error("Supabase client not initialized in claim_referral_bonus.")
        return jsonify({'success': False, 'message': 'Backend setup issue: Supabase client not initialized.'}), 500
    
    data = request.json
    user_id = data.get('userId')

    if not user_id:
        return jsonify({'success': False, 'message': 'User ID is required.'}), 400

    try:
        # Fetch current pending bonus and wallet balance
        # --- MODIFIED: Added 'order_income' to the select statement ---
        wallet_response = supabase.table('user_wallets') \
                                 .select('balance, pending_referral_bonus, total_referral_earnings, order_income') \
                                 .eq('user_id', user_id) \
                                 .single() \
                                 .execute()
        
        if not wallet_response.data:
            return jsonify({'success': False, 'message': 'User wallet not found.'}), 404

        current_balance = wallet_response.data['balance']
        pending_bonus = wallet_response.data['pending_referral_bonus']
        total_referral_earnings = wallet_response.data['total_referral_earnings']
        current_order_income = wallet_response.data['order_income'] # --- NEW: Fetch current order_income ---

        if pending_bonus <= 0:
            return jsonify({'success': False, 'message': 'No pending bonus to claim.'}), 400

        # Calculate new balances
        amount_to_claim = pending_bonus
        new_balance = current_balance + amount_to_claim
        new_total_referral_earnings = total_referral_earnings + amount_to_claim
        new_order_income = current_order_income + amount_to_claim # --- NEW: Calculate new order_income ---

        # Update wallet: add to balance, reset pending, update total earned and order_income
        # --- MODIFIED: Added 'order_income' to the update dictionary ---
        update_wallet_response = supabase.table('user_wallets').update({
            'balance': new_balance,
            'pending_referral_bonus': 0.0,
            'total_referral_earnings': new_total_referral_earnings,
            'order_income': new_order_income
        }).eq('user_id', user_id).execute()

        if not update_wallet_response.data:
            app_logger.error(f"Failed to update wallet for claiming referral bonus for user {user_id}. Supabase error: {update_wallet_response.error}")
            return jsonify({'success': False, 'message': 'Failed to update wallet after claiming bonus.'}), 500

        # Record a transaction for the bonus claim
        transaction_data = {
            'user_id': user_id,
            'amount': amount_to_claim,
            'type': 'bonus_referral_signup',
            'status': 'completed',
            'description': f'Claimed ₹{amount_to_claim} referral signup bonus'
        }
        supabase.table('transactions').insert(transaction_data).execute() # Log this, but don't block response on it

        app_logger.info(f"User {user_id} claimed ₹{amount_to_claim} referral bonus.")
        # --- MODIFIED: Added 'new_order_income' to the response ---
        return jsonify({
            'success': True,
            'message': f'₹{amount_to_claim} referral bonus claimed successfully!',
            'new_balance': new_balance,
            'new_pending_bonus': 0.0,
            'new_total_referral_earnings': new_total_referral_earnings,
            'new_order_income': new_order_income
        }), 200

    except Exception as e:
        app_logger.error(f"Error claiming referral bonus for user {user_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred while claiming bonus.'}), 500


@app.route('/api/manual-payment/confirm', methods=['POST'])
def confirm_manual_payment():
    """
    Endpoint for users to submit their manual UPI payment details (UTR, amount, mobile number)
    for admin verification. Now includes a Telegram notification.
    """
    data = request.get_json()
    user_id = data.get('userId')
    amount = data.get('amount')
    utr_number = data.get('utrNumber')
    mobile_number = data.get('mobileNumber')

    if not all([user_id, amount, utr_number, mobile_number]):
        app_logger.error("Missing required fields for manual payment confirmation.")
        return jsonify({'success': False, 'message': 'Missing required payment details.'}), 400

    # Basic data validation
    if not (isinstance(amount, (int, float)) and len(str(utr_number)) == 12 and len(str(mobile_number)) == 10):
        app_logger.error("Invalid data format for manual payment.")
        return jsonify({'success': False, 'message': 'Invalid data format.'}), 400

    try:
        # Insert the payment details into a new table for manual verification
        payment_data = {
            'user_id': user_id,
            'amount': amount,
            'utr_number': utr_number,
            'mobile_number': mobile_number,
            'status': 'pending'
        }
        
        insert_response = supabase.table('manual_payments').insert(payment_data).execute()
        
        app_logger.info(f"Manual payment submitted for user {user_id} with UTR: {utr_number}")

        # --- NEW: Send a Telegram notification to the admin ---
        notification_message = (
            f"🔔 <b>New Payment Submitted</b>\n"
            f"<b>User ID:</b> <code>{user_id}</code>\n"
            f"<b>Amount:</b> ₹{amount}\n"
            f"<b>UTR:</b> <code>{utr_number}</code>\n"
            f"Status: <b>Pending Admin Verification</b>"
        )
        send_telegram_notification(notification_message)
        # ---------------------------------------------------

        return jsonify({
            'success': True,
            'message': 'Payment details submitted successfully. Awaiting admin verification.'
        }), 201

    except Exception as e:
        app_logger.error(f"Internal Server Error while confirming manual payment: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred.'}), 500


# --- NEW: Get All Pending Manual Payments Endpoint ---
@app.route('/api/admin/manual-payments/pending', methods=['GET'])
def get_pending_manual_payments():
    """
    Admin-only endpoint to fetch all pending manual payment records.
    This route must be protected with admin authentication.
    """
    try:
        # Fetch all records with a 'pending' status from the manual_payments table
        response = supabase.table('manual_payments') \
            .select('*') \
            .eq('status', 'pending') \
            .order('created_at', desc=True) \
            .execute()
        
        if response.data:
            app_logger.info(f"Fetched {len(response.data)} pending manual payment requests.")
            return jsonify({'success': True, 'records': response.data}), 200
        else:
            app_logger.info("No pending manual payment requests found.")
            return jsonify({'success': True, 'records': []}), 200
    
    except Exception as e:
        app_logger.error(f"Error fetching pending payments: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'Failed to fetch records.'}), 500


@app.route('/api/admin/manual-payment/reject', methods=['POST'])
def reject_manual_payment():
    """
    Admin-only endpoint to reject a manual payment and mark it as 'rejected'.
    This route handles multiple pending payments submitted with the same UTR number.
    """
    try:
        # data from request
        data = request.get_json()
        utr_number = data.get('utr_number')
        
        if not utr_number:
            app_logger.error("Missing UTR number for manual payment rejection.")
            return jsonify({'success': False, 'message': 'UTR number is required.'}), 400

        # Fetch all pending records for the UTR, not just a single one
        response = supabase.table('manual_payments') \
                          .select('id') \
                          .eq('utr_number', utr_number) \
                          .eq('status', 'pending') \
                          .execute()

        if not response.data:
            app_logger.error(f"No pending payments found for UTR: {utr_number}")
            return jsonify({'success': False, 'message': 'No pending payment found for this UTR number.'}), 404

        # Update each payment record found to 'rejected'
        for payment_record in response.data:
            supabase.table('manual_payments') \
                    .update({'status': 'rejected'}) \
                    .eq('id', payment_record['id']) \
                    .execute()
        
        app_logger.info(f"Manual payments with UTR {utr_number} successfully rejected.")
        return jsonify({'success': True, 'message': f'Payment requests for UTR {utr_number} rejected.'}), 200

    except APIError as e:
        app_logger.error(f"PostgREST API error during manual payment rejection: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'A database error occurred.'}), 500
    except Exception as e:
        app_logger.error(f"Error rejecting payment for UTR {utr_number}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred.'}), 500



# --- VERIFY Manual Payment Endpoint (from previous conversation) ---
@app.route('/api/admin/manual-payment/verify', methods=['POST'])
def admin_verify_manual_payment():
    """
    Admin-only endpoint to verify and complete a manual payment.
    This route handles multiple payments submitted with the same UTR number,
    but ensures the amount is credited only once.
    """
    try:
        data = request.get_json()
        utr_number = data.get('utr_number')
        
        if not utr_number:
            app_logger.error("Missing UTR number for manual payment verification.")
            return jsonify({'success': False, 'message': 'UTR number is required.'}), 400

        # Fetch all pending records for the UTR
        response = supabase.table('manual_payments') \
                            .select('user_id,amount,id,is_credited') \
                            .eq('utr_number', utr_number) \
                            .eq('status', 'pending') \
                            .execute()
        
        if not response.data:
            app_logger.error(f"No pending payments found for UTR: {utr_number}")
            return jsonify({'success': False, 'message': 'No pending payment found for this UTR number.'}), 404

        first_payment_processed = False
        
        # Iterate and process all pending records for the same UTR
        for payment_data in response.data:
            user_id = payment_data['user_id']
            recharge_amount_inr = payment_data['amount']
            payment_id = payment_data['id']
            is_credited = payment_data.get('is_credited', False)

            # Check if this UTR has already been credited
            if is_credited:
                app_logger.info(f"UTR {utr_number} has already been credited. Skipping wallet update for payment ID {payment_id}.")
            
            # This is the first time we're processing this UTR, so credit the user and mark the record.
            elif not first_payment_processed:
                # 1. Update the manual payment record to 'completed' and mark as credited
                supabase.table('manual_payments') \
                        .update({'status': 'completed', 'is_credited': True}) \
                        .eq('id', payment_id) \
                        .execute()
                app_logger.info(f"Manual payment {utr_number} for user {user_id} marked as 'completed' and credited.")
                
                # 2. --- REFERRAL COMMISSION LOGIC ---
                try:
                    referrer_response = supabase.table('profiles') \
                                                .select('referrer_id') \
                                                .eq('id', user_id) \
                                                .single() \
                                                .execute()
                    
                    referrer_id = referrer_response.data.get('referrer_id') if referrer_response.data else None
                    
                    if referrer_id:
                        app_logger.info(f"User {user_id} was referred by {referrer_id}. Calculating commission.")
                        commission_amount = float(recharge_amount_inr) * 0.10
                        
                        commission_rpc_response = supabase.rpc('increment_referral_commission', {
                            'p_user_id': referrer_id,
                            'p_amount': commission_amount
                        }).execute()

                        if commission_rpc_response.status_code == 204:
                            app_logger.info(f"Commission of {commission_amount} credited to referrer {referrer_id}.")
                            commission_log_data = {
                                'referrer_id': referrer_id,
                                'referred_user_id': user_id,
                                'commission_amount': commission_amount,
                                'investment_amount': float(recharge_amount_inr)
                            }
                            supabase.table('commissions').insert(commission_log_data).execute()
                            app_logger.info(f"Commission log created for referrer {referrer_id}.")
                        else:
                            app_logger.error(f"Failed to credit commission for referrer {referrer_id}. RPC response: {commission_rpc_response.status_code}")

                except Exception as commission_error:
                    app_logger.error(f"Error processing referral commission for user {user_id} on manual payment: {commission_error}", exc_info=True)
                
                # 3. --- UPDATE USER'S WALLET ---
                try:
                    rpc_response = supabase.rpc('increment_recharged_amount', {
                        'p_user_id': user_id,
                        'p_amount': float(recharge_amount_inr)  
                    }).execute()
                    app_logger.info(f"Wallet 'recharged_amount' updated for user {user_id} via RPC.")
                except Exception as rpc_exec_error:
                    app_logger.error(f"Failed to execute RPC 'increment_recharged_amount' for {user_id}. Error: {rpc_exec_error}", exc_info=True)
                    raise Exception("Supabase RPC 'increment_recharged_amount' failed...") from rpc_exec_error

            
                # -----------------------------------------------

                first_payment_processed = True
            
            # For all subsequent pending requests with the same UTR, just mark them as completed
            else:
                supabase.table('manual_payments') \
                        .update({'status': 'completed'}) \
                        .eq('id', payment_id) \
                        .execute()
                app_logger.info(f"Manual payment {utr_number} for user {user_id} marked as 'completed' without crediting.")

        return jsonify({
            'success': True,
            'message': 'Manual payments successfully verified and wallet credited once for this UTR.'
        }), 200

    except Exception as e:
        app_logger.error(f"Internal Server Error during manual payment verification for UTR {utr_number}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred during verification.'}), 500


# --- NEW: Create Razorpay Order Endpoint ---
# This endpoint is called by the frontend to get an order_id before opening the Razorpay popup.
@app.route('/api/recharge/create-razorpay-order', methods=['POST'])
def create_razorpay_order():
    if not razorpay_client:
        app_logger.error("Razorpay client not initialized in create_razorpay_order.")
        return jsonify({'success': False, 'message': 'Backend setup issue: Razorpay client not initialized.'}), 500

    data = request.get_json()
    amount_in_inr = data.get('amount')
    user_id = data.get('userId')

    if not all([amount_in_inr, user_id]):
        return jsonify({'success': False, 'message': 'Amount and User ID are required to create an order.'}), 400

    amount_in_paisa = int(float(amount_in_inr) * 100)

    try:
        # Generate a short, unique receipt ID using UUID
        # A UUID is 36 characters, which fits within the 40-character limit.
        # Optionally, you can prefix it with something short like 'rcpt_' if you want,
        # but the raw UUID is unique enough.
        receipt_id = str(uuid.uuid4()) # Generates a unique UUID like 'a1b2c3d4-e5f6-7890-1234-567890abcdef'
        app_logger.info(f"Generated Razorpay receipt ID: {receipt_id}")

        order_payload = {
            'amount': amount_in_paisa,
            'currency': 'INR',
            'receipt': receipt_id, # Use the generated UUID here
            'payment_capture': '1'
        }
        razorpay_order = razorpay_client.order.create(order_payload)
        app_logger.info(f"Razorpay order created: {razorpay_order['id']} for user {user_id}, amount {amount_in_inr}")
        
        # ... (rest of your create_razorpay_order logic, including pending transaction insert) ...
        pending_transaction_data = {
            'user_id': user_id,
            'type': 'recharge',
            'amount': amount_in_inr, # Store in INR
            'status': 'pending',
            'description': f'Razorpay order creation for {amount_in_inr} INR',
            'payment_gateway_id': razorpay_order['id'], # Store Razorpay order ID
            'receipt_id': receipt_id # Store the receipt ID for reference if needed
        }
        supabase.table('transactions').insert(pending_transaction_data).execute()
        app_logger.info(f"Pending transaction recorded for Razorpay order {razorpay_order['id']}")


        return jsonify({
            'success': True,
            'order_id': razorpay_order['id'],
            'amount': amount_in_inr,
            'currency': 'INR',
            'key_id': RAZORPAY_KEY_ID
        }), 200

    except Exception as e:
        app_logger.error(f"Error creating Razorpay order: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'Failed to create Razorpay order: {e}'}), 500



# --- MODIFIED: Verify Razorpay Payment Endpoint ---
@app.route('/api/recharge/verify-razorpay-payment', methods=['POST'])
def verify_razorpay_payment():
    if not razorpay_client or not supabase:
        app_logger.error("Clients not initialized in verify_razorpay_payment.")
        return jsonify({'success': False, 'message': 'Backend setup issue: Clients not initialized.'}), 500

    data = request.get_json()
    razorpay_order_id = data.get('razorpay_order_id')
    razorpay_payment_id = data.get('razorpay_payment_id')
    razorpay_signature = data.get('razorpay_signature')
    recharge_amount_inr = data.get('amount')
    user_id = data.get('userId')

    if not all([razorpay_order_id, razorpay_payment_id, razorpay_signature, recharge_amount_inr, user_id]):
        app_logger.error(f"Missing payment details in request: {data}")
        return jsonify({'success': False, 'message': 'Missing payment details.'}), 400

    try:
        # Verify the payment signature
        razorpay_client.utility.verify_payment_signature({
            'razorpay_order_id': razorpay_order_id,
            'razorpay_payment_id': razorpay_payment_id,
            'razorpay_signature': razorpay_signature
        })
        app_logger.info(f"Razorpay signature verified for payment {razorpay_payment_id}")
        
        # --- NEW: BEGIN REFERRAL COMMISSION LOGIC ---
        try:
            # 1. Look up the referrer for the user who just recharged
            referrer_response = supabase.table('profiles') \
                                        .select('referrer_id') \
                                        .eq('id', user_id) \
                                        .single() \
                                        .execute()
            
            referrer_id = referrer_response.data.get('referrer_id') if referrer_response.data else None

            if referrer_id:
                app_logger.info(f"User {user_id} was referred by {referrer_id}. Calculating commission.")

                # 2. Calculate the commission (10% of the recharge amount)
                commission_amount = float(recharge_amount_inr) * 0.10
                
                # 3. Use the NEW RPC to update the referrer's wallet
                # This RPC will now update BOTH total_referral_earnings and order_income
                commission_rpc_response = supabase.rpc('increment_referral_commission', {
                    'p_user_id': referrer_id,
                    'p_amount': commission_amount
                }).execute()

                if commission_rpc_response.status_code == 204:
                    app_logger.info(f"Commission of {commission_amount} credited to referrer {referrer_id}'s total earnings AND withdrawable balance (order_income).")
                    
                    # 4. Log the commission in the 'commissions' table for an audit trail
                    commission_log_data = {
                        'referrer_id': referrer_id,
                        'referred_user_id': user_id,
                        'commission_amount': commission_amount,
                        'investment_amount': float(recharge_amount_inr)
                    }
                    supabase.table('commissions').insert(commission_log_data).execute()
                    app_logger.info(f"Commission log created for referrer {referrer_id} for referral {user_id}.")
                else:
                    app_logger.error(f"Failed to credit commission for referrer {referrer_id}. RPC response: {commission_rpc_response.status_code}")

        except Exception as commission_error:
            # IMPORTANT: We catch this error separately so a failure to credit commission
            # doesn't block the user's main transaction from completing successfully.
            # We log it and move on. You might want to add an alert for admin here.
            app_logger.error(f"Error processing referral commission for user {user_id}: {commission_error}", exc_info=True)
            # The rest of the transaction logic proceeds as normal
        # --- END NEW REFERRAL COMMISSION LOGIC ---

        # The rest of your existing code continues here, but slightly reorganized
        
        # --- Your existing logic to update the user's wallet (recharged_amount) ---
        app_logger.info(f"Calling RPC 'increment_recharged_amount' for user {user_id} with amount {recharge_amount_inr}")
        try:
            rpc_response = supabase.rpc('increment_recharged_amount', {
                'p_user_id': user_id,
                'p_amount': float(recharge_amount_inr) 
            }).execute()
            app_logger.info(f"Wallet 'recharged_amount' updated via RPC for {user_id}. Supabase response was successful.")
        except Exception as rpc_exec_error:
            app_logger.error(f"Failed to execute RPC 'increment_recharged_amount' for {user_id}. Error: {rpc_exec_error}", exc_info=True)
            raise Exception("Supabase RPC 'increment_recharged_amount' failed...") from rpc_exec_error

        # Update the pending transaction status to 'completed'
        update_transaction_response = supabase.table('transactions') \
            .update({'status': 'completed', 'payment_gateway_id': razorpay_payment_id}) \
            .eq('payment_gateway_id', razorpay_order_id) \
            .execute()
        
        if not update_transaction_response.data:
            app_logger.error(f"Failed to update transaction status for order {razorpay_order_id}. Supabase error: {update_transaction_response.error}")

        # Now, fetch the latest balance to return to the frontend
        latest_wallet_response = supabase.table('user_wallets').select('recharged_amount').eq('user_id', user_id).single().execute()
        latest_recharged_amount = latest_wallet_response.data['recharged_amount'] if latest_wallet_response.data else 0
        app_logger.info(f"Confirmed new recharged_amount by separate fetch: {latest_recharged_amount}")

        return jsonify({'success': True, 'message': 'Recharge successful and wallet updated!', 'new_recharged_amount': latest_recharged_amount})

    except razorpay.errors.SignatureVerificationError as e:
        app_logger.error(f"Razorpay Signature Verification Failed for order {razorpay_order_id}: {e}", exc_info=True)
        supabase.table('transactions').update({'status': 'failed', 'description': f'Payment verification failed: {e}'}).eq('payment_gateway_id', razorpay_order_id).execute()
        return jsonify({'success': False, 'message': 'Payment verification failed: Invalid signature.'}), 400
    except Exception as e:
        app_logger.error(f"Internal Server Error during payment verification for order {razorpay_order_id}: {e}", exc_info=True)
        supabase.table('transactions').update({'status': 'failed', 'description': f'Internal server error: {e}'}).eq('payment_gateway_id', razorpay_order_id).execute()
        return jsonify({'success': False, 'message': f'Payment verification failed due to an unexpected error.'}), 500
    

# src/app.py

@app.route('/api/user/recharge-records/<uuid:user_id>', methods=['GET'])
def get_recharge_records(user_id):
    """
    Endpoint to fetch manual recharge records for a specific user.
    """
    try:
        # Query the 'manual_payments' table instead of 'transactions'
        response = supabase.table('manual_payments') \
            .select('amount, status, created_at, utr_number') \
            .eq('user_id', user_id) \
            .order('created_at', desc=True) \
            .execute()
            
        records = response.data
        
        return jsonify({'success': True, 'records': records}), 200

    except Exception as e:
        app_logger.error(f"Error fetching recharge records for user {user_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred.'})
        
    
@app.route('/api/user/withdrawal-records/<uuid:user_id>', methods=['GET'])
def get_withdrawal_records(user_id):
    # WARNING: This endpoint is public. For a production app, you must add authentication.

    try:
        response = supabase.table('transactions') \
            .select('id, amount, status, created_at, fee, bank_card_id') \
            .eq('user_id', user_id) \
            .eq('type', 'withdrawal') \
            .order('created_at', desc=True) \
            .execute()
        
        # Handle cases where `fee` might not be present by providing a default value of 0
        records = [{**record, 'fee': record.get('fee', 0)} for record in response.data]

        return jsonify({'success': True, 'records': records}), 200
        
    except Exception as e:
        app_logger.error(f"Error fetching withdrawal records for user {user_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred.'}), 500

    
@app.route('/api/user/add-bank-card', methods=['POST'])
def add_bank_card():
    try:
        data = request.json
        user_id = data.get('user_id')
        account_number = data.get('account_number')
        bank_name = data.get('bank_name')
        ifsc_code = data.get('ifsc_code')
        account_holder_name = data.get('account_holder_name')

        if not all([user_id, account_number, bank_name, ifsc_code, account_holder_name]):
            return jsonify({'success': False, 'message': 'Missing required bank card details.'}), 400

        # Basic IFSC validation
        if not (isinstance(ifsc_code, str) and len(ifsc_code) == 11 and ifsc_code.isalnum() and ifsc_code[0:4].isalpha() and ifsc_code[4] == '0' and ifsc_code[5:].isalnum()):
            return jsonify({'success': False, 'message': 'Invalid IFSC Code format.'}), 400

        response = supabase.table('bank_cards').insert({
            'user_id': user_id,
            'account_number': account_number,
            'bank_name': bank_name,
            'ifsc_code': ifsc_code,
            'account_holder_name': account_holder_name,
            'is_verified': False,
            'razorpay_fund_account_id': None # Keep this column, it's harmless and can be used later if you get RazorpayX
        }).execute()

        if response and response.data and len(response.data) > 0:
            return jsonify({
                'success': True,
                'message': 'Bank card added successfully!',
                'bank_card_id': response.data[0]['id']
            }), 201
        else:
            app_logger.error(f"Supabase insert returned no data unexpectedly for bank card. Response: {response.error if hasattr(response, 'error') else 'No data or error'}")
            return jsonify({'success': False, 'message': 'Failed to save bank card. No data returned from Supabase.'}), 500

    except Exception as e:
        app_logger.error(f"Error adding bank card: {e}", exc_info=True)
        error_message = 'An unexpected error occurred.'
        if hasattr(e, 'message'):
            error_message = e.message
        elif hasattr(e, 'response') and hasattr(e.response, 'text'):
            try:
                error_json = e.response.json()
                error_message = error_json.get('message', error_message)
            except:
                error_message = e.response.text
        elif str(e):
            error_message = str(e)

        return jsonify({'success': False, 'message': f'Failed to add bank card: {error_message}'}), 500

@app.route('/api/user/bank-cards/<user_id>', methods=['GET'])
def get_user_bank_cards(user_id):
    try:
        response = supabase.table('bank_cards').select('*').eq('user_id', user_id).execute()

        if response.data is None:
            return jsonify({'success': True, 'bank_cards': []}), 200

        # Note: response.count is typically for queries with .limit() or .range() and .count() methods.
        # For a simple select without count, checking response.data is usually sufficient.
        if response.error: # Check for a direct error from Supabase
            app_logger.error(f"Supabase error fetching bank cards: {response.error.message}")
            return jsonify({'success': False, 'message': 'Database error fetching bank cards.'}), 500

        return jsonify({'success': True, 'bank_cards': response.data}), 200

    except Exception as e:
        app_logger.error(f"Error fetching bank cards for user {user_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred.'}), 500

@app.route('/api/user/set-trade-password', methods=['POST'])
def set_trade_password():
    try:
        data = request.json
        user_id = data.get('user_id')
        new_trade_password = data.get('new_trade_password')

        if not all([user_id, new_trade_password]):
            return jsonify({'success': False, 'message': 'User ID and new trade password are required.'}), 400

        if len(new_trade_password) < 6:
            return jsonify({'success': False, 'message': 'Trade password must be at least 6 characters long.'}), 400

        hashed_trade_password = generate_password_hash(new_trade_password)

        response = supabase.table('profiles').update({
            'trade_password_hash': hashed_trade_password
        }).eq('id', user_id).execute()

        if response and response.data and len(response.data) > 0:
            return jsonify({'success': True, 'message': 'Trade password set successfully!'}), 200
        else:
            app_logger.error(f"Supabase update returned no data unexpectedly for trade password. Response: {response.error if hasattr(response, 'error') else 'No data or error'}")
            return jsonify({'success': False, 'message': 'Failed to set trade password. No data returned from Supabase.'}), 500

    except Exception as e:
        app_logger.error(f"Error setting trade password: {e}", exc_info=True)
        error_message = 'An unexpected error occurred.'
        if hasattr(e, 'message'):
            error_message = e.message
        elif hasattr(e, 'response') and hasattr(e.response, 'text'):
            try:
                error_json = e.response.json()
                error_message = error_json.get('message', error_message)
            except:
                error_message = e.response.text
        elif str(e):
            error_message = str(e)

        return jsonify({'success': False, 'message': f'Failed to set trade password: {error_message}'}), 500

@app.route('/api/user/verify-trade-password', methods=['POST'])
def verify_user_password():
    try:
        data = request.json
        user_id = data.get('userId')
        submitted_trade_password = data.get('password')

        if not all([user_id, submitted_trade_password]):
            return jsonify({'success': False, 'message': 'User ID and trade password are required.'}), 400

        try:
            profile_response = supabase.table('profiles').select('trade_password_hash').eq('id', user_id).single().execute()
        except APIError as e:
            if e.code == 'PGRST116': # Not Found error code for PostgREST
                app_logger.warning(f"Trade password not set for user {user_id}. No profile found or no hash.")
                return jsonify({'success': False, 'message': 'Trade password not set for user.'}), 404
            else:
                app_logger.error(f"Supabase API error fetching profile for trade password verification: {e.message}", exc_info=True)
                return jsonify({'success': False, 'message': f'Database error during trade password verification: {e.message}'}), 500
        except Exception as e:
            app_logger.error(f"Unexpected error during Supabase call for trade password verification: {e}", exc_info=True)
            return jsonify({'success': False, 'message': 'An unexpected database error occurred.'}), 500

        if profile_response.data and profile_response.data['trade_password_hash']:
            stored_trade_password_hash = profile_response.data['trade_password_hash']
            if check_password_hash(stored_trade_password_hash, submitted_trade_password):
                return jsonify({'success': True, 'message': 'Trade password verified.'}), 200
            else:
                return jsonify({'success': False, 'message': 'Invalid trade password.'}), 401
        else:
            app_logger.warning(f"Trade password hash is NULL for user {user_id}.")
            return jsonify({'success': False, 'message': 'Trade password not set for user.'}), 404

    except Exception as e:
        app_logger.error(f"Error verifying trade password (general exception): {e}", exc_info=True)
        error_message = 'An unexpected error occurred during trade password verification.'
        if hasattr(e, 'message'):
            error_message = e.message
        elif hasattr(e, 'response') and hasattr(e.response, 'text'):
            try:
                error_json = e.response.json()
                error_message = error_json.get('message', error_message)
            except:
                error_message = e.response.text
        elif str(e):
            error_message = str(e)

        return jsonify({'success': False, 'message': f'Trade password verification failed: {error_message}'}), 500


@app.route('/api/user/has-successful-investment', methods=['GET'])
def check_successful_investment():
    try:
        user_id = request.args.get('userId')
        if not user_id:
            return jsonify({'hasInvested': False}), 400

        # Query the manual_payment table for any successful payments by this user
        response = supabase.table('manual_payments') \
    .select('id') \
    .eq('user_id', user_id) \
    .in_('status', ['success', 'completed']) \
    .limit(1) \
    .execute()

        # If any data is returned, it means a successful payment exists
        if response.data and len(response.data) > 0:
            return jsonify({'hasInvested': True}), 200
        else:
            return jsonify({'hasInvested': False}), 200

    except Exception as e:
        app_logger.error(f"Error checking for successful investment for user {user_id}: {e}", exc_info=True)
        return jsonify({'hasInvested': False}), 500

@app.route('/api/withdrawal/request', methods=['POST'])
def handle_withdrawal_request():
    try:
        data = request.json
        user_id = data.get('userId')
        amount = data.get('amount')
        bank_card_id = data.get('bankCardId')
        bank_details = data.get('bankDetails')

        if not all([user_id, amount, bank_card_id, bank_details]):
            return jsonify({'success': False, 'message': 'Missing withdrawal details.'}), 400

        if not isinstance(amount, (int, float)) or amount <= 0:
            return jsonify({'success': False, 'message': 'Invalid withdrawal amount.'}), 400

        # NEW: Check for withdrawal frequency
        today = datetime.date.today()
        start_of_day = datetime.datetime.combine(today, datetime.time.min, tzinfo=datetime.timezone.utc)
        
        withdrawal_count_response = supabase.table('transactions').select('id').eq('user_id', user_id).eq('type', 'withdrawal').gt('created_at', start_of_day.isoformat()).execute()
        
        if withdrawal_count_response.data and len(withdrawal_count_response.data) >= 2:
            app_logger.warning(f"Withdrawal failed: User {user_id} has exceeded the daily withdrawal limit.")
            return jsonify({'success': False, 'message': 'You can only withdraw twice per day.'}), 403

        # NEW: Check if the user has a successful investment before proceeding
        investment_check_response = supabase.table('manual_payments') \
                                            .select('id') \
                                            .eq('user_id', user_id) \
                                            .in_('status', ['success', 'completed']) \
                                            .limit(1) \
                                            .execute()
        if not investment_check_response.data or len(investment_check_response.data) == 0:
            app_logger.warning(f"Withdrawal failed: User {user_id} has no successful investments.")
            return jsonify({'success': False, 'message': 'You must have a successful investment to withdraw.'}), 403

        # 1. Fetch current order_income from the user's wallet
        wallet_response = supabase.table('user_wallets').select('order_income').eq('user_id', user_id).single().execute()
        if not wallet_response.data:
            app_logger.warning(f"User wallet not found for withdrawal for user: {user_id}")
            return jsonify({'success': False, 'message': 'User wallet not found.'}), 404

        current_order_income = wallet_response.data['order_income']

        # 2. Calculate Fee and Final Amount
        fee_rate = 0.12
        withdrawal_fee = round(amount * fee_rate, 2)
        total_amount_to_deduct = round(amount, 2)

        print(f"DEBUG: Current income in DB: {current_order_income}, Withdrawal requested: {total_amount_to_deduct}")

        # Check if withdrawal amount exceeds the order_income
        if current_order_income < total_amount_to_deduct:
            return jsonify({'success': False, 'message': 'Insufficient order income for withdrawal.'}), 400

        # 3. Deduct amount from order_income immediately
        new_order_income = current_order_income - total_amount_to_deduct
        wallet_update_response = supabase.table('user_wallets').update({'order_income': new_order_income}).eq('user_id', user_id).execute()

        if not wallet_update_response.data or len(wallet_update_response.data) == 0:
            app_logger.error(f"Failed to update wallet balance for withdrawal for user {user_id}.")
            return jsonify({'success': False, 'message': 'Failed to update wallet balance for withdrawal.'}), 500

        app_logger.info(f"Wallet updated for {user_id}. New order income: {new_order_income}. Recording withdrawal request.")

        # 4. Record Transaction with 'pending' status
        transaction_data = {
            'user_id': user_id,
            'amount': total_amount_to_deduct,
            'fee': withdrawal_fee,
            'type': 'withdrawal',
            'status': 'pending',
            'description': f"Withdrawal request for {total_amount_to_deduct} INR (processing)",
            'bank_card_id': bank_card_id,
            'metadata': bank_details
        }
        transaction_response = supabase.table('transactions').insert(transaction_data).execute()

        if not transaction_response.data or len(transaction_response.data) == 0:
            app_logger.error(f"Failed to record pending withdrawal transaction for user {user_id}. Supabase response: {transaction_response.error if hasattr(transaction_response, 'error') else 'No data or error'}")
            app_logger.error(f"Attempting to refund {total_amount_to_deduct} to user {user_id} due to transaction record failure.")
            supabase.table('user_wallets').update({'order_income': current_order_income}).eq('user_id', user_id).execute()
            return jsonify({'success': False, 'message': 'Failed to record withdrawal request. Amount refunded to wallet. Please try again or contact support.'}), 500

        app_logger.info(f"Withdrawal request recorded as pending for user {user_id}. Transaction ID: {transaction_response.data[0]['id']}")

        account_number = bank_details.get('accountNumber', 'N/A')
        bank_name = bank_details.get('bankName', 'N/A')

        # --- NEW: Send Telegram notification for withdrawal request ---
        notification_message = (
            f"💸 <b>New Withdrawal Request!</b>\n"
            f"<b>User ID:</b> <code>{user_id}</code>\n"
            f"<b>Amount:</b> ₹{total_amount_to_deduct}\n"
            f"<b>Withdrawal Fee:</b> ₹{withdrawal_fee}\n"
            f"<b>Bank Account:</b> {bank_details.get('bankName')} ending in {bank_details.get('accountNumber')[-4:]}\n"
            f"Status: <b>Pending Admin Approval</b>"
        )
        send_telegram_notification(notification_message)
        # -------------------------------------------------------------

        return jsonify({
            'success': True,
            'message': f'Withdrawal request for ₹{total_amount_to_deduct} submitted. It is now processing.',
            'new_order_income': new_order_income,
            'transaction_id': transaction_response.data[0]['id']
        }), 200

    except Exception as e:
        app_logger.error(f"Unhandled error in withdrawal request for user {user_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'message': 'An unexpected error occurred during withdrawal request submission.'}), 500


TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '7840580443:AAE1UQPFopt9OQdjYjkTWJzhsFQ3NFpcl5s')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '5952225695') # <-- Use the chat ID you found

def send_telegram_notification(message_body):
    """Sends a Telegram message to the specified chat ID."""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message_body,
            'parse_mode': 'HTML' # Allows for basic HTML formatting
        }
        response = requests.post(url, data=payload)
        response.raise_for_status()
        app_logger.info("Telegram notification sent successfully.")
        return True
    except requests.exceptions.RequestException as e:
        app_logger.error(f"Failed to send Telegram notification: {e}")
        return False
        
@app.route('/ping')
def ping():
    return "pong", 200

# --- Main execution block ---
if __name__ == '__main__':
    PORT = int(os.environ.get("PORT", 5000))
    app_logger.info(f"Flask app starting on port {PORT}")
    app.run(host='0.0.0.0', debug=True, port=PORT)
