def confirm_checkout(request):
    """Finalize a checkout by applying session changes to the database.

    This view consumes items stored in the user's checkout session, updates
    room assignments, item/project reservations, and logs item movements.
    It also sends alert emails when items are effectively "stolen" from
    another project's reservations and cleans up temporary session data.
    """

    # Pull the project_id off the request body.
    data = json.loads(request.body)
    project_id = data.get("project_id")

    # This is the project we're checking items into.
    new_project = Project.objects.get(project_id=project_id)

    if "checkout_session_item_us" in request.session:
        item_id_list = []  # Keep track of item_ids whose quantities we need to recalc later.
        checkout_session_item_us = request.session["checkout_session_item_us"]
        stolen_projects = []  # Track any projects we're effectively stealing reservations from.

        for item_u_id in checkout_session_item_us:
            # Grab the specific item_u and its base Items record.
            item_u = ItemsU.objects.get(item_u_id=item_u_id)
            item_id_list.append(item_u.item_id)
            item = Items.objects.get(item_id=item_u.item_id)

            # CASE: Item is available or reserved on a different project and we're moving it here.
            if item_u.stage == "a" or (
                item_u.stage == "r" and int(item_u.project_id) != int(project_id)
            ):
                # Make sure the target project has an "Unassigned" room to drop this into.
                unassigned_room, created = Room.objects.get_or_create(
                    project_id=project_id,
                    room_name="Unassigned",
                    defaults={"details": "Auto-created unassigned room"},
                )
                unassigned_room.save()

                # Either create or bump the ItemRoom record for this item in Unassigned.
                item_room, created = ItemRoom.objects.get_or_create(
                    item_id=item.item_id,
                    room_id=unassigned_room.room_id,
                )

                if not created:
                    item_room.item_quantity += 1
                    item_room.item_quantity_holder += 1
                    item_room.all_reserved += 1
                else:
                    item_room.item_quantity = 1
                    item_room.item_quantity_holder = 1
                    item_room.all_reserved = 1

                item_room.save()

                # Sync ItemProject.reserved_quantity for this item on the new project.
                ip, _ = ItemProject.objects.get_or_create(item=item, project=new_project)
                ip.reserved_quantity = sum(
                    ir.item_quantity
                    for ir in ItemRoom.objects.filter(item=item, room__project=new_project)
                )
                ip.save()

                # Wipe out any ItemRoom rows that landed at zero.
                ItemRoom.objects.filter(item_quantity__lt=1).delete()

                # If it was reserved on another project, back out that project's counts.
                if item_u.stage == "r" and int(item_u.project_id) != int(project_id):
                    previous_item_room = ItemRoom.objects.filter(
                        item_id=item_u.item_id,
                        room__project_id=item_u.project_id,
                    ).first()

                    if previous_item_room:
                        previous_item_room.item_quantity -= 1
                        previous_item_room.item_quantity_holder -= 1
                        previous_item_room.all_reserved -= 1

                        if previous_item_room.item_quantity < 1:
                            previous_item_room.delete()
                        else:
                            previous_item_room.save()

                        # Also refresh reserved_quantity on the previous project.
                        prev_project = previous_item_room.room.project
                        ip_prev, _ = ItemProject.objects.get_or_create(
                            item=item, project=prev_project
                        )
                        ip_prev.reserved_quantity = sum(
                            ir.item_quantity
                            for ir in ItemRoom.objects.filter(
                                item=item,
                                room__project=prev_project,
                            )
                        )
                        ip_prev.save()

                    item.save()

                    # Track how many reservations we "stole" so we can email folks later.
                    stolen_project_exists = False
                    for project_info in stolen_projects:
                        if project_info["project_id"] == item_u.project_id:
                            stolen_project_exists = True
                            stolen_item_exists = False

                            for item_dict in project_info["items"]:
                                if item_dict["sku_num"] == item.sku_num:
                                    item_dict["quantity"] += 1
                                    stolen_item_exists = True
                                    break

                            if not stolen_item_exists:
                                project_info["items"].append(
                                    {"sku_num": item.sku_num, "quantity": 1}
                                )
                            break

                    if not stolen_project_exists:
                        project_dict = {
                            "project_id": item_u.project_id,
                            "items": [{"sku_num": item.sku_num, "quantity": 1}],
                        }
                        stolen_projects.append(project_dict)

            # CASE: Item is in "move" stage; free up a reserved copy and park it in Unassigned.
            elif item_u.stage == "m":
                # Try to grab another available copy of this item to swap into its spot.
                new_item = ItemsU.objects.filter(item_id=item_u.item_id, stage="a").first()
                if new_item:
                    new_item.stage = "m"
                    new_item.project_id = None
                    new_item.save()

                unassigned_room, created = Room.objects.get_or_create(
                    project_id=project_id,
                    room_name="Unassigned",
                    defaults={"details": "Auto-created unassigned room"},
                )
                unassigned_room.save()

                item_room, created = ItemRoom.objects.get_or_create(
                    item_id=item.item_id,
                    room_id=unassigned_room.room_id,
                )

                if not created:
                    item_room.item_quantity += 1
                    item_room.item_quantity_holder += 1
                    item_room.all_reserved += 1
                else:
                    item_room.item_quantity = 1
                    item_room.item_quantity_holder = 1
                    item_room.all_reserved = 1

                item_room.save()

            # Any other stage (besides 'r', which we already handled) means something's off.
            elif item_u.stage != "r":
                context = {
                    "success": False,
                    "message": "There was an issue confirming this checkout session.",
                }
                return JsonResponse(context)

            # Try to log this checkout action for the item.
            try:
                project = Project.objects.get(project_id=project_id)
                create_item_log("c", item_u, project)
            except Exception:
                # If logging blows up, don't tank the whole checkout.
                pass

            # Finally, mark this specific item as checked out to the project.
            item_u.stage = "c"
            item_u.project_id = project_id
            item_u.save()

        # Send alert emails to any projects that had reservations pulled away.
        for project in stolen_projects:
            user_emails = User.objects.filter(
                id__in=UserProject.objects.filter(
                    project_id=project["project_id"]
                ).values_list("user_id", flat=True)
            ).values_list("email", flat=True)
            superuser_emails = User.objects.filter(is_superuser=True).values_list(
                "email", flat=True
            )
            recipient_emails = list(set(user_emails).union(set(superuser_emails)))

            stolen_project = Project.objects.get(project_id=project["project_id"])

            for item_dict in project["items"]:
                item = Items.objects.get(sku_num=item_dict["sku_num"])
                email_subject = "Alert: Item Reservation Adjusted"
                email_body = render_to_string(
                    "stolen_item_email.html",
                    {
                        "new_project_name": new_project.project_name,
                        "project_name": stolen_project.project_name,
                        "item_name": item.studio_id,
                        "sku": item.sku_num,
                        "item": item,
                        "adjusted_by": request.user.get_full_name(),
                        "adjusted_quantity": item_dict["quantity"],
                    },
                )
                email = EmailMessage(
                    subject=email_subject,
                    body=email_body,
                    from_email=config("EMAIL_HOST_USER"),
                    to=recipient_emails,
                )
                email.content_subtype = "html"
                email.send()

        # Recalc aggregate quantities for every base item we touched.
        for item_id in item_id_list:
            update_item_quantities(item_id, project_id)

        # Clear out the checkout session list of item_us.
        del request.session["checkout_session_item_us"]

    # Also clear any older/misc checkout session container if it's hanging around.
    if "checkout_session_items" in request.session:
        del request.session["checkout_session_items"]

    # Final cleanup pass: drop ItemRoom rows where all_reserved slipped below 1.
    ItemRoom.objects.filter(all_reserved__lt=1).delete()

    context = {"success": True}
    return JsonResponse(context)