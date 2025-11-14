def add_inventory(request):
    """Process a bulk Studio inventory upload and stage changes in session.

    This view:
    - Validates that there are no in-progress projects or pending manual updates.
    - Reads the uploaded Studio Excel report (including embedded images).
    - Compares Studio data with the current Items table.
    - Determines which items are new, changed, deleted, or need quantity adjustment.
    - Generates QR labels for quantity increases.
    - Stores all proposed changes in the session for later confirmation.
    """

    # Make sure our Item quantity rollups are current before we touch the new upload.
    Items.objects.update(total_quantity=F("available_quantity") + F("reserved_quantity"))
    Items.objects.filter(previous_total_quantity__lt=F("total_quantity")).update(
        previous_total_quantity=F("total_quantity")
    )

    # Quick sanity check: make sure it's a POST and we actually got a file.
    if request.method != "POST" or not request.FILES.get("inventory_file"):
        return JsonResponse({
            "success": False,
            "message": "Invalid Request",
        })

    # Don't let a new upload run if there are completed projects with checked-out items
    # that Studio hasn't caught up with yet.
    projects = (
        Project.objects.filter(
            itemproject__checked_out_quantity__gt=0,
            completed_date__isnull=False,
        )
        .exclude(itemproject__checked_out_quantity_holder=99999)
        .distinct()
    )
    if projects.exists():
        return JsonResponse({
            "success": False,
            "message": "Please update Studio with necessary changes before uploading a new report.",
        })

    # Also block uploads if there are still manual quantity changes waiting to be pushed to Studio.
    manual_df_path = os.path.join(settings.MEDIA_ROOT, "manual_quantity_changes.csv")
    if os.path.exists(manual_df_path) and not pd.read_csv(manual_df_path).empty:
        return JsonResponse({
            "success": False,
            "message": "Please finish updating items to Studio before uploading a new report.",
        })

    # Grab the uploaded Studio Excel file off the request.
    excel_file = request.FILES["inventory_file"]

    # Save a copy of the upload to disk so we have it for audit/backup.
    file_path = os.path.join(settings.MEDIA_ROOT, "studio_report.xlsx")
    with open(file_path, "wb") as destination:
        for chunk in excel_file.chunks():
            destination.write(chunk)

    # Open the workbook and map any embedded images to the row they live on.
    wb = load_workbook(excel_file, data_only=True)
    ws = wb.active
    image_data_map = {}
    for img in ws._images:
        try:
            row_idx = img.anchor._from.row
            image_bytes = img._data() if hasattr(img, "_data") else img.image
            encoded = base64.b64encode(image_bytes).decode("utf-8")
            image_data_map[row_idx] = encoded
        except Exception:
            # If an image gives us trouble, just skip it and keep processing the rest.
            continue

    # Pull the sheet data into a pandas DataFrame.
    df = pd.read_excel(
        excel_file,
        engine="openpyxl",
        header=1,
        keep_default_na=True,
        na_values=["", "NA", "N/A", "null", "NULL", "nan", "None"],
    )

    # Normalize Inventory Ids and flag any duplicates in the incoming file.
    df["normalized"] = df["Inventory Id"].str.lower().str.strip()
    dupes = df[df["normalized"].duplicated(keep=False)]
    if len(dupes) > 0:
        return JsonResponse({
            "success": False,
            "message": (
                "Duplicate Inventory Ids found: "
                f"{', '.join(dupes['Inventory Id'].unique())}. "
                "Please review Studio report and resolve duplicates before proceeding."
            ),
        })

    df = df.fillna("")
    df["excel_row"] = df.index + 2  # Excel rows start at 1, plus one header row.
    df.columns = df.columns.str.strip()

    # Only keep rows where Studio says some quantity was actually ordered.
    df = df[df["Quantity Ordered"] > 0].reset_index(drop=True)

    # Buckets to track what changed: brand-new items, field tweaks, deletes, and quantity drops.
    new_items = {}         # sku -> field values for brand new items
    items_to_change = {}   # sku -> {field: new_value}
    items_to_delete = []   # [sku,...] items no longer present in Studio
    items_to_subtract = {} # sku -> quantity to remove from our side

    # Used to make sure any new sku we generate doesn't collide with existing Studio IDs.
    studio_ids = list(
        Items.objects.values_list("studio_id", flat=True).distinct()
    )

    # First pass: look for Items we have that no longer show up in Studio.
    # If any of those are still reserved, we bail out with an error.
    for studio_id in studio_ids:
        if studio_id not in df["Inventory Id"].str.strip().unique().tolist():
            items = Items.objects.filter(studio_id=studio_id)
            for item in items:
                item.total_quantity = item.available_quantity + item.reserved_quantity
                if item.total_quantity > 0:
                    item_u = ItemsU.objects.filter(item_id=item.item_id, stage="r").first()
                    if item_u:
                        return JsonResponse({
                            "success": False,
                            "message": (
                                f"{studio_id} is not found in Studio upload, and at least one "
                                "of this item is currently reserved to a project. "
                                "This issue is irreconcilable."
                            ),
                        })
                    else:
                        items_to_delete.append(item.sku_num)

    all_qr_codes = []  # Collect all QR images so we can dump them into one PDF.

    # Tiny helper to normalize nullable string fields from the DB.
    def norm(val):
        """Normalize None → '' and always return a stripped string."""
        return (val or "").strip()

    # Second pass: walk every row from Studio and compare it against our Items table.
    for _, row in df.iterrows():
        image_data = image_data_map.get(row["excel_row"])
        image = None

        # If there's an image, make sure it looks like valid base64 before we keep it.
        if image_data:
            try:
                base64.b64decode(image_data)
                image = image_data
            except binascii.Error:
                # If image fails to decode, ignore it instead of failing the whole upload.
                pass

        # Check if this Studio row already maps to an existing Item in our DB.
        item = Items.objects.filter(
            studio_id=str(row["Inventory Id"]).strip()
        ).first()

        if item:
            # --- Existing item: clean up quantity fields and look for metadata changes. ---
            item_fixes = False

            # Make sure all the quantity fields are non-null and add up the way we expect.
            if item.available_quantity is None:
                item_fixes = True
                item.available_quantity = 0
            if item.reserved_quantity is None:
                item_fixes = True
                item.reserved_quantity = 0
            if item.total_quantity is None:
                item_fixes = True
                item.total_quantity = item.available_quantity + item.reserved_quantity
            if (item.available_quantity + item.reserved_quantity) != item.total_quantity:
                item_fixes = True
                item.total_quantity = item.available_quantity + item.reserved_quantity

            if item_fixes:
                item.save()

            # Little helper to tack on a field change for this sku.
            def register_change(field_name: str, new_value):
                items_to_change.setdefault(item.sku_num, {})[field_name] = new_value

            # Walk through the other fields and note anything that changed.
            if norm(item.description) != str(row["Description"].strip()):
                register_change("description", str(row["Description"].strip()))
            if round(float(item.sale_price), 2) != round(float(row["Selling Price"]), 2):
                register_change("sale_price", round(float(row["Selling Price"]), 2))
            if round(float(item.unit_cost), 2) != round(float(row["Total Cost"]), 2):
                register_change("unit_cost", round(float(row["Total Cost"]), 2))
            if norm(item.vendor) != str(row["Vendor"]).strip():
                register_change("vendor", str(row["Vendor"].strip()))
            if norm(item.dimensions).replace('"', "") != str(row["Dimensions"]).strip().replace('"', ""):
                register_change("dimensions", str(row["Dimensions"].strip()))
            if norm(item.sales_code) != str(row["Sales Code"]).strip():
                register_change("sales_code", str(row["Sales Code"].strip()))
            if norm(item.location) != str(row["Location"]).strip():
                register_change("location", str(row["Location"].strip()))
            if norm(item.item_code_1) != str(row["Item Code 1"]).strip():
                register_change("item_code_1", str(row["Item Code 1"].strip()))
            if norm(item.item_code_2) != str(row["Item Code 2"]).strip():
                register_change("item_code_2", str(row["Item Code 2"].strip()))
            if norm(item.category) != str(row["Sub Category"]).strip():
                register_change("category", str(row["Sub Category"].strip()))

            # Check if Studio now says we have more available than our previous_total_quantity.
            studio_available = int(float(row["Quantity Available"]))
            if studio_available > item.previous_total_quantity:
                labels_needed = studio_available - item.previous_total_quantity
                qr_images = qr_code_generator(
                    request,
                    request.user,
                    str(row["Description"]).strip(),
                    item.sku_num,
                    str(row["Location"]).strip(),
                    labels_needed,
                    source="add_inventory",
                )
                all_qr_codes.extend(qr_images)

            # If Studio shows fewer available than our total, we'll need to subtract on our side.
            elif studio_available < item.total_quantity:
                labels_to_remove = item.total_quantity - studio_available
                items_to_subtract[item.sku_num] = labels_to_remove

        else:
            # --- Brand new item: generate a fresh sku and stage it for creation. ---
            labels_needed = int(float(row["Quantity Available"]))

            # Spin up a random 8-digit sku and make sure it doesn't collide with Studio IDs we already know about.
            sku = ""
            valid_sku = False
            while not valid_sku:
                sku = str(random.randint(10_000_000, 99_999_999))
                if sku not in studio_ids:
                    valid_sku = True

            if labels_needed > 0:
                qr_images = qr_code_generator(
                    request,
                    request.user,
                    str(row["Description"]).strip(),
                    sku,
                    str(row["Location"]).strip(),
                    labels_needed,
                    source="add_inventory",
                )
                all_qr_codes.extend(qr_images)

            # Build out the new item payload using all the relevant Studio fields.
            new_items[sku] = {
                "description": str(row["Description"]).strip(),
                "studio_id": str(row["Inventory Id"]).strip(),
                "sale_price": round(float(row["Selling Price"]), 2),
                "unit_cost": round(float(row["Total Cost"]), 2),
                "vendor": str(row["Vendor"]).strip(),
                "dimensions": str(row["Dimensions"]).strip(),
                "sales_code": str(row["Sales Code"]).strip(),
                "location": str(row["Location"]).strip(),
                "item_code_1": str(row["Item Code 1"]).strip(),
                "item_code_2": str(row["Item Code 2"]).strip(),
                "category": str(row["Sub Category"]).strip(),
                "available_quantity": labels_needed,
                "available_quantity_holder": labels_needed,
                "total_quantity": labels_needed,
                "total_quantity_holder": labels_needed,
                "previous_total_quantity": labels_needed,
                "previous_total_quantity_holder": labels_needed,
            }

            if image:
                new_items[sku]["image"] = image

    # If we generated any QR labels, bundle them into a single PDF for printing.
    if len(all_qr_codes) > 0:
        qr_pdf_generator(request.user, all_qr_codes, "add_inventory")
        pdf_path = os.path.join(settings.MEDIA_URL, "qr_stickers.pdf")
    else:
        pdf_path = None

    # Drop a log entry so we know a new inventory upload ran.
    create_log("NI", request.user, description="")

    # Stash all the detected changes in session so the user can confirm or tweak them later.
    request.session["new_items"] = new_items
    request.session["items_to_change"] = items_to_change
    request.session["items_to_delete"] = items_to_delete
    request.session["items_to_subtract"] = items_to_subtract

    # Figure out if anything actually changed based on the upload.
    changes_detected = not (
        len(new_items) == 0
        and len(items_to_change) == 0
        and len(items_to_delete) == 0
        and len(items_to_subtract) == 0
    )

    return JsonResponse({
        "changes": changes_detected,
        "pdf_path": pdf_path,
        "success": True,
        "message": "File processed successfully",
    })
