# consumer/services/reconciliation_service.py

from scrapers.models import Performance
from scrapers.models.pos import POSListing
from scrapers.models.seat_packs import SeatPack
from .pos_api_client import POSAPIClient

class ReconciliationService:
    def __init__(self, performance_id):
        self.performance = Performance.objects.get(internal_performance_id=performance_id)
        self.pos_api_client = POSAPIClient()

    def run(self):
        """Main entry point for the reconciliation process."""
        active_listings = POSListing.objects.filter(
            performance=self.performance,
            status='ACTIVE'
        )

        # Get the latest scrape job for this performance to avoid duplicate data
        latest_scrape_job = SeatPack.objects.filter(
            zone_id__performance_id=self.performance
        ).order_by('-created_at').first()
        
        if not latest_scrape_job:
            print(f"No seat packs found for performance {self.performance.internal_performance_id}")
            return

        scraped_packs = SeatPack.objects.filter(
            zone_id__performance_id=self.performance,
            scrape_job_key=latest_scrape_job.scrape_job_key
        )

        for listing in active_listings:
            self.reconcile_listing(listing, scraped_packs)

    def reconcile_listing(self, listing, scraped_packs):
        """Reconciles a single active POS listing against the scraped state."""
        
        db_packs = listing.seat_packs.all()

        db_pack_set = {f"{pack.row_label}-{pack.start_seat_number}-{pack.end_seat_number}" for pack in db_packs}
        scraped_pack_set = {f"{pack.row_label}-{pack.start_seat_number}-{pack.end_seat_number}" for pack in scraped_packs}

        if db_pack_set == scraped_pack_set:
            print(f"Listing {listing.pos_inventory_id}: No changes detected.")
            return

        removed_packs = db_pack_set - scraped_pack_set
        if removed_packs:
            print(f"Listing {listing.pos_inventory_id}: Packs {removed_packs} removed. Initiating split.")

        unchanged_packs = db_pack_set.intersection(scraped_pack_set)

        if not unchanged_packs:
            print(f"Listing {listing.pos_inventory_id}: All packs removed. Deactivating listing.")
            listing.status = 'INACTIVE'
            listing.save()
            return

        payload = {
            "splitInventoryTicketsRequests": [
                {"ticketIds": [pack.source_pack_id for pack in db_packs if f"{pack.row_label}-{pack.start_seat_number}-{pack.end_seat_number}" in unchanged_packs]}
            ]
        }

        response = self.pos_api_client.split_inventory(
            listing.pos_inventory_id,
            payload
        )

        if response.is_successful:
            self._update_database_after_split(listing, response.data)
        else:
            print(f"API Error splitting {listing.pos_inventory_id}: {response.error}")

    def _update_database_after_split(self, old_listing, api_response):
        old_listing.status = 'SPLIT'
        old_listing.save()

        for new_inventory in api_response.get('new_inventories', []):
            new_pos_listing = POSListing.objects.create(
                performance=self.performance,
                pos_inventory_id=new_inventory.get('pos_inventory_id'),
                status='ACTIVE'
            )

            # Update the seat packs to point to the new listing
            SeatPack.objects.filter(
                source_pack_id__in=new_inventory.get('ticket_ids', [])
            ).update(pos_listing=new_pos_listing)
        
        print(f"Successfully split {old_listing.pos_inventory_id} into {len(api_response.get('new_inventories', []))} new listings.")
